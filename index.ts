import express from 'express';
import { PrismaClient } from '@prisma/client';
import amqp from 'amqplib';
import { v4 as uuidv4 } from 'uuid';

const prisma = new PrismaClient();
const app = express();
app.use(express.json());

const RABBITMQ_URL = 'amqp://localhost';
const QUEUE_NAME = 'tasks';

interface Task {
  id: string;
  priority: number;
  payload: any;
  scheduledAt: Date | null;
}

async function connectRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  await channel.assertQueue(QUEUE_NAME, { durable: true });
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
  await channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify({ id: task.id })));

  res.json({ message: 'Task created successfully', taskId: task.id });
});

async function executeTask(task: Task) {
  console.log(`Executing task ${task.id} with priority ${task.priority}`);
  
  // Implement your task logic here based on priority
  switch (task.priority) {
    case 1:
      await executeHighPriorityTask(task);
      break;
    case 2:
      await executeMediumPriorityTask(task);
      break;
    case 3:
      await executeLowPriorityTask(task);
      break;
    default:
      console.log(`Unknown priority: ${task.priority}`);
  }

  await prisma.job.update({
    where: { id: task.id },
    data: { status: 'COMPLETED' },
  });
}

async function executeHighPriorityTask(task: Task) {
  console.log('Executing high priority task', task.payload);
  // Implement high priority task logic
}

async function executeMediumPriorityTask(task: Task) {
  console.log('Executing medium priority task', task.payload);
  // Implement medium priority task logic
}

async function executeLowPriorityTask(task: Task) {
  console.log('Executing low priority task', task.payload);
  // Implement low priority task logic
}

async function worker() {
  const { channel } = await connectRabbitMQ();

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