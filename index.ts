import express from 'express';
import { PrismaClient } from '@prisma/client';
import amqp from 'amqplib';

const prisma = new PrismaClient();
const app = express();
app.use(express.json());

const RABBITMQ_URL = 'amqp://localhost';
const QUEUE_NAME = 'tasks2';

interface Task {
  id: string;
  priority: number;
  payload: any;
  scheduledAt: Date | null;
}

// Simulated API rate limit
let apiCallCount = 0;
const API_RATE_LIMIT = 5;
const API_RATE_LIMIT_WINDOW = 60000; // 1 minute

async function connectRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  await channel.assertQueue(QUEUE_NAME, { durable: true, maxPriority: 10 });
  return { connection, channel };
}

app.post('/task', async (req, res) => {
  const { priority, payload, scheduledAt } = req.body;
  
  const task = await prisma.job.create({
    data: {
      priority,
      payload,
      scheduledAt: scheduledAt ? new Date(scheduledAt) : null,
    },
  });

  const { channel } = await connectRabbitMQ();
  await channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify({ id: task.id })), {
    persistent: true,
    priority: priority
  });

  res.json({ message: 'Task created successfully', taskId: task.id });
});

async function executeTask(task: Task) {
  console.log(`Executing task ${task.id} with priority ${task.priority}`);
  
  const startTime = Date.now();
  
  switch (task.payload.action) {
    case 'fetch_data':
      await fetchData(task);
      break;
    case 'send_email':
      await sendEmail(task);
      break;
    case 'process_image':
      await processImage(task);
      break;
    default:
      console.log(`Unknown action: ${task.payload.action}`);
  }

  const endTime = Date.now();
  const executionTime = endTime - startTime;

  console.log(`Task ${task.id} completed in ${executionTime}ms`);

  await prisma.job.update({
    where: { id: task.id },
    data: { 
      status: 'COMPLETED',
    },
  });

  console.log(`Task ${task.id} completed in ${executionTime}ms`);
}

async function fetchData(task: Task) {
  console.log('Fetching data', task.payload);
  
  if (apiCallCount >= API_RATE_LIMIT) {
    console.log('Rate limit reached. Rescheduling task.');
    await rescheduleTask(task);
    return;
  }

  apiCallCount++;
  setTimeout(() => apiCallCount--, API_RATE_LIMIT_WINDOW);

  // Simulate API call
  await new Promise(resolve => setTimeout(resolve, 1000));
  console.log(`Data fetched for ${task.payload.entity}`);
}

async function sendEmail(task: Task) {
  console.log('Sending email', task.payload);
  // Simulate sending an email
  await new Promise(resolve => setTimeout(resolve, 2000));
  console.log(`Email sent to ${task.payload.recipient}`);
}

async function processImage(task: Task) {
  console.log('Processing image', task.payload);
  // Simulate image processing
  await new Promise(resolve => setTimeout(resolve, 3000));
  console.log(`Image ${task.payload.imageUrl} processed`);
}

async function rescheduleTask(task: Task) {
  const rescheduleTime = new Date(Date.now() + 60000); // Reschedule after 1 minute
  await prisma.job.update({
    where: { id: task.id },
    data: { 
      scheduledAt: rescheduleTime,
      status: 'RESCHEDULED'
    },
  });

  const { channel } = await connectRabbitMQ();
  await channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify({ id: task.id })), {
    persistent: true,
    priority: task.priority
  });
}

async function worker() {
  const { channel } = await connectRabbitMQ();

  channel.prefetch(1); // Ensure the worker processes one task at a time

  channel.consume(QUEUE_NAME, async (msg) => {
    if (msg !== null) {
      const { id } = JSON.parse(msg.content.toString());
      
      const task = await prisma.job.findUnique({ where: { id } });
      
      if (task && (task.scheduledAt === null || task.scheduledAt <= new Date())) {
        await executeTask(task);
        channel.ack(msg);
      } else {
        // Requeue the message if it's not ready to be executed
        channel.nack(msg, false, true);
      }
    }
  });
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
  worker().catch(console.error);
});