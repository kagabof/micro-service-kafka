const express = require('express');
const bodyParser = require('body-parser')
const { randomBytes } = require('crypto');
const { request } = require('http');
const cors = require('cors');
const axios = require('axios');
const {Kafka} = require('kafkajs');
const dotenv = require('dotenv');

const app = express();
app.use(bodyParser.json());
app.use(cors())
dotenv.config()


const { KAFKA_UNAME: username, KAFKA_PWORD: password } = process.env
const sasl = username && password ? { username, password, mechanism: 'plain' } : null
const ssl = !!sasl

// This creates a client instance that is configured to connect to the Kafka broker provided by
const kafka = new Kafka({
//   clientId: 'npm-slack-notifier',
  brokers: [process.env.KAFKA_SERVER],
  ssl,
  sasl
});

  const producer = kafka.producer()
  producer.connect().then(() => {
      console.log('connected ......')
  });

const connect = async() => {
    try {
    const data = {
        transaction_id: 1,
        ledger_ref: 795,
        transaction_status: 'completed',
        paymentrefno: 0
      }
    const success = await producer.send({
        topic: 'test1',
        messages: [
          { value: JSON.stringify(data) },
        ],
      });
      console.log('success...>>>>>>.', success)
    } catch (error) {
        console.log('..>>>>../////', error)
    }
}


setInterval(() => {
    connect();
}, 5000);


const posts = {
    member_id: 7866,
    linked_msisdn: "250780282575",
};

app.get('/posts', (req, res) => {
    res.json(posts);
});

app.post('/posts', async (req, res) => {
    const id = randomBytes(4).toString('hex');
    const { title } = req.body;
    posts[id] = {
        id, title
    }
    // await axios.post('http://localhost:4005/events', {
    //     type: 'PostCreated',
    //     data: {
    //         id, title,
    //     }
    // });

    return res.status(201).json(posts[id])
});

app.post('/events', async(req, res) => {
    console.log('....?????? received events post');
    res.send({});
})

app.listen(4001, async () => {
    console.log('Post service listening on: http://127.0.0.1:4000');
    // await producer.connect();
    // await producer.send({
    //     topic: 'test',
    //     messages: 'Connection established...',
    // })
    // await producer.disconnect()

})