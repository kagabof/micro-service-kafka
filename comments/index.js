const express = require('express');
const bodyParser = require('body-parser')
const { randomBytes } = require('crypto');
// const { request } = require('http');
const cors = require('cors');
const axios = require('axios');
const {Kafka} = require('kafkajs');
const dotenv = require('dotenv');

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

const app = express();
app.use(bodyParser.json());
app.use(cors())

// const kafka = new Kafka({
//     clientId: 'my-app',
//     brokers: ['localhost:9092']
// });
const consumer = kafka.consumer({ groupId: 'kafka' });

consumer.connect().then(() => {
    console.log('consumer connected ......');
});

consumer.subscribe({
    topic: 'savings',
    fromBeginning: true }).then(() => {
        console.log('consumer subscribe connected ......')
});
const res = {
    flag: '',
    data: {
        transaction_id: 11111111111,
        ledger_ref: 729,
        transaction_status: 'completed',
        paymentrefno: 0
      }
}
const connect = async() => {
    await consumer.run({
        eachMessage: async (data) => {
          console.log('>>>>>', JSON.parse(data.message.value.toString()))
        },
      })
}

connect();

const commentsByPostId = {};

app.get('/posts/:id/comments', (req, res) => {
    res.send(commentsByPostId[req.params.id] || []);
});

app.post('/posts/:id/comments', async (req, res) => {
    const commentId = randomBytes(4).toString('hex');
    const { content } = req.body;
    const comments = commentsByPostId[req.params.id] || [];
    comments.push({ id: commentId, content });
    commentsByPostId[req.params.id] = comments;

    await axios.post('http://localhost:4005/events', {
        type: 'CommentCreated',
        data: {
            id: commentId,
            postId: req.params.id,
            content
        }
    });

    res.status(201).send(comments);
});

app.post('/events', async(req, res) => {
    console.log('....?????? received events')
    res.send({});
})

app.listen(7000, async () => {
    console.log('Comments service is listening on: http://127.0.0.1:4001');
    connect();
});
