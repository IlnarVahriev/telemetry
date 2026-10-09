const Database = require('better-sqlite3');
const fs = require('fs');

const rawData = fs.readFileSync('telemetry.json', 'utf8');
const objectsPool = JSON.parse(rawData);

console.log('Загружено объектов:', objectsPool.length);


const db = new Database('telemetry.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS telemetry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER,
    object_name TEXT,
    object_type TEXT,
    value TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS trend_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_name TEXT,
    value TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  );
`);


const insertTelemetryStmt = db.prepare(
  'INSERT INTO telemetry (source_id, object_name, object_type, value) VALUES (?, ?, ?, ?)'
);

const insertTrendStmt = db.prepare(
  'INSERT INTO trend_logs (object_name, value) VALUES (?, ?)'
);


const TREND_NAMES = new Set([
  'motor_speed',
  'motor_current',
  'motor_voltage',
  'motor_torque',
  'motor_temperature',
  'dc_bus_voltage',
  'phase_current_a',
  'phase_current_b',
  'phase_current_c',
  'phase_voltage_a',
  'phase_voltage_b',
  'phase_voltage_c',
  'frequency',
  'power_factor',
  'active_power',
  'reactive_power',
  'apparent_power',
  'energy_total',
  'energy_daily',
  'temperature_drive'
]);

console.log('Трендов задано:', TREND_NAMES.size);


class RingBufferWAL {
  constructor(maxSize) {
    this.maxSize = maxSize;  
    this.buffer = [];        
  }

  push(record) {
    if (this.buffer.length >= this.maxSize) {
      this.buffer.shift();
    }

    this.buffer.push(record);
    insertTrendStmt.run(record.objectName, String(record.value));
  }

  getBuffer() {
    return this.buffer;
  }
}

const trendWal = new RingBufferWAL(3600);


function validateValue(type, value) {
  const t = type ? type.toLowerCase() : '';

  if (t === 'bool' || t === 'boolean') {
    return typeof value === 'boolean';
  }

  if (t === 'byte' || t === 'int') {
    return typeof value === 'number' && Number.isInteger(value);
  }

  if (t === 'float' || t === 'double') {
    return typeof value === 'number' && !isNaN(value);
  }

  return false;
}


function generateBatch() {
  const count = Math.floor(Math.random() * (600 - 100 + 1)) + 100;
  const batch = [];

  for (let i = 0; i < count; i++) {
    const randomIndex = Math.floor(Math.random() * objectsPool.length);
    const item = Object.assign({}, objectsPool[randomIndex]);

    if (Math.random() < 0.05) {
      item.value = 'BAD_VALUE';
    }

    batch.push(item);
  }

  return batch;
}


const SOURCES_COUNT = 4;
const VALUES_PER_SOURCE_LIMIT = 1000;
const sourceCounters = [0, 0, 0, 0]; 

const intervalId = setInterval(() => {
  let activeSources = 0;

  for (let sourceId = 0; sourceId < SOURCES_COUNT; sourceId++) {
    if (sourceCounters[sourceId] >= VALUES_PER_SOURCE_LIMIT) {
      continue;
    }

    activeSources++;
    const batch = generateBatch();

    for (const item of batch) {
      if (sourceCounters[sourceId] >= VALUES_PER_SOURCE_LIMIT) {
        break;
      }

      if (validateValue(item.type, item.value)) {
        insertTelemetryStmt.run(
          sourceId + 1,
          item.name,
          item.type,
          String(item.value)
        );

        if (TREND_NAMES.has(item.name)) {
          trendWal.push({
            objectName: item.name,
            value: item.value,
            timestamp: new Date()
          });
        }

        sourceCounters[sourceId]++;
      } else {
        console.warn(
          `[ОШИБКА ВАЛИДАЦИИ] Источник ${sourceId + 1}, ` +
          `объект "${item.name}"`
        );
      }
    }

    console.log(
      `Источник ${sourceId + 1}: собрано ${sourceCounters[sourceId]} / ${VALUES_PER_SOURCE_LIMIT}`
    );
  }

  if (activeSources === 0) {
    console.log('Сбор данных окончен!');
    console.log('Размер WAL в памяти:', trendWal.getBuffer().length);
    clearInterval(intervalId);
    db.close();
  }
}, 1000);