const Database = require('better-sqlite3');
const fs = require('fs');

const fileData = fs.readFileSync('telemetry.json', 'utf8');
const objectsPool = JSON.parse(fileData);

console.log('Всего объектов в пуле:', objectsPool.length);

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

const insertTelemetry = db.prepare('INSERT INTO telemetry (source_id, object_name, object_type, value) VALUES (?, ?, ?, ?)');
const insertTrend = db.prepare('INSERT INTO trend_logs (object_name, value) VALUES (?, ?)');

const trendNames = [
  'motor_speed', 'motor_current', 'motor_voltage', 'motor_torque',
  'motor_temperature', 'dc_bus_voltage', 'phase_current_a', 'phase_current_b',
  'phase_current_c', 'phase_voltage_a', 'phase_voltage_b', 'phase_voltage_c',
  'frequency', 'power_factor', 'active_power', 'reactive_power',
  'apparent_power', 'energy_total', 'energy_daily', 'temperature_drive'
];

let walJournal = [];
const MAX_WAL_SIZE = 3600;

function addToWal(name, val) {
  if (walJournal.length >= MAX_WAL_SIZE) {
    walJournal.shift();
  }
  walJournal.push({ name: name, value: val, time: new Date() });
  
  insertTrend.run(name, String(val));
}

function isValidValue(type, value) {
  if (type === 'boolean' || type === 'bool') {
    return typeof value === 'boolean';
  }
  if (type === 'int' || type === 'byte' || type === 'float' || type === 'double') {
    return typeof value === 'number';
  }
  return false;
}

const SOURCES_COUNT = 4;
const LIMIT_VALUES = 1000;
let sourceCounters = [0, 0, 0, 0];

console.log('Запуск сбора телеметрии');

const interval = setInterval(() => {
  let activeSources = 0;

  for (let srcId = 0; srcId < SOURCES_COUNT; srcId++) {
    if (sourceCounters[srcId] >= LIMIT_VALUES) {
      continue;
    }

    activeSources++;

    let batchSize = Math.floor(Math.random() * (600 - 100 + 1)) + 100;

    for (let i = 0; i < batchSize; i++) {
      if (sourceCounters[srcId] >= LIMIT_VALUES) {
        break;
      }

      let randomIndex = Math.floor(Math.random() * objectsPool.length);
      let item = objectsPool[randomIndex];

      let currentValue = item.value;
      if (Math.random() < 0.05) {
        currentValue = "BAD_VALUE";
      }

      if (isValidValue(item.type, currentValue)) {
        insertTelemetry.run(srcId + 1, item.name, item.type, String(currentValue));

        if (trendNames.includes(item.name)) {
          addToWal(item.name, currentValue);
        }

        sourceCounters[srcId]++;
      } else {
        console.log(`[ЛОГ ОШИБКИ] Источник ${srcId + 1}: неверное значение для ${item.name}`);
      }
    }

    console.log(`Источник ${srcId + 1} собрал значений: ${sourceCounters[srcId]} / ${LIMIT_VALUES}`);
  }

  if (activeSources === 0) {
    console.log('Сбор данных завершен!');
    console.log('Текущий размер WAL журнала в памяти:', walJournal.length);
    clearInterval(interval);
    db.close();
    console.log('База данных закрыта.');
  }

}, 1000);
