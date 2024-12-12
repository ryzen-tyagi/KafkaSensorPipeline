require('dotenv').config();
const { Kafka } = require('kafkajs');
const mysql = require('mysql2');

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: 'sensor-group' });

const db = mysql.createConnection({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

const consumeMessages = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: process.env.KAFKA_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      console.log('Data received from Kafka:', data);

      db.query(
        'INSERT INTO sensor_readings (deviceId, temperature, humidity, smokeLevel, gps, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
        [data.deviceId, data.temperature, data.humidity, data.smokeLevel, data.gps, new Date()],
        (err, results) => {
          if (err) console.error(err);
          else console.log('Data inserted into DB:', results);
        }
      );
    },
  });
};

consumeMessages().catch(console.error);
