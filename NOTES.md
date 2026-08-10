
This is my understanding of the project
The project is trying to solve one problem.
Imagine you're running a print shop.

A customer walks in.
"Please print my 300-page thesis."
Should the receptionist stand there and print 300 pages?
No.

Because while she's printing...
Nobody else can order anything.
Instead she does this:
Customer comes with Job or task.
The receptionist takes the task or job
Logs it
Gives a receipt
Waits for the next customer

Elsewhere:
the worker  -> checks the next order -> and carries the order->

I've noticed that there are two processes: Accept orders and Process orders.
I learned it is called decoupling.
The receptionist doesn't print.
The printer doesn't greet customers.

What happens when the printer isn't available? 
The response should be: No problem.
We'll keep your order safely.
When the printer returns,
it'll continue.

Redis is the storage system that owns the queue.

Just like people waiting in a line, a queue is first in -> first out (FIFO).
Like people waiting in line.

Redis is a server. It is a program running on my computer.
When Redis starts 
It waits for the clients instruction, then it responds: okay (please correct me if I'm wrong)
When the worker asks for the next Job Redis says here
And that's all
Redis is another application (Would really really love to know more about this, its pros and cons why someone would use this in a project) (This is why I would love a lot of projects so I can make decisions for managing memory, optimizing, trade off, like why I should pick one stuff over another and then making the most of the resources I have for speed security and other non-funcional requirements, adding quality assurance and quality control)

Docker runs redis, it does stuff like creating an isolated computer and in that computer Redis runs (why is it isolated, so it can still do stuff when the power goes down? Where are the other times we'll use this)
Docker saves me from installing everything manually

Why persistence:
Redis stores data in RAm and the RAM forgets everything when the power goes off so if we want to have the feature where we tell the customer that  No problem. It'll keep my order safely.When the printer returns, it'll continue.
Docker let's Redis save its data to a folder on my computer so that it can do this. Docker on its own doesn't automatically do it.

When Redis starts again when the power comes on:
Disk -> Reload's RAM

This feature is called persistence not fault tolerance or resilience

Redis supports it using things like
RDB snapshots
AOF (Append Only File)
I don't need to memorize those yet.

I just need to know:
Memory
↓
Disk
↓
Memory again

Understanding post /jobs:
So, someone sends POST /jobs 
inside my server
receive request -> Generate Job ID -> Store Job -> Return ID
The worker is a completely different program (would love to know why. Finishing wes bos 30 days for Js used to doing everything in one file, would like to know when it is proper to create another file and folder structures to)


The worker constantly asks Redis
Any jobs? Redis Yes. Job #248 Worker Thanks. Processes it. Done. Then asks again. Any jobs? Forever.

Why sleep(ms)
ecause we don't actually have real work yet.

Instead of Resize Video 30 seconds we fake it. await sleep(5000)
Five seconds later...Finished. The worker isn't useful yet. It's proving Background processing works.

Later
instead of
sleep()

I'll have
resize image
send email
generate PDF
AI inference
convert video
upload S3

(Would love to actually implement this later so I'd actually understand what's going on)

What if power goes off?
Excellent.
Redis persistence saves
Waiting Jobs
to disk.
Power returns.
Redis reloads.
Worker reconnects.
Continues. Exactly as I imagined. That's called durably

The thought that the conveyor belt handles tasks one by one is cap. The queue stores them one by one the number of workers determines how many jobs are handled/ processed simultaneously



"I'm thinking Express or NEST for the API. Nest because I heard its neat. One POST endpoint that takes a JSON body with a 'duration' field. The worker is just a separate file in the same project folder that listens to the BullMQ queue. The fake job is a setTimeout that waits for however many seconds the API sent."

Would love a what is the purpose and importance of this project and if it would be used to solve other problems and where it would be applied



Would like a notes of the techologies used maybe after the full projects and when to use them for certain scenarios (where they'd shine mostly)

Each backend project becomes a case study, just like my JavaScript30 projects have become. Alongside the code, keep:

README.md — what we built and how to run it.
NOTES.md — concepts and terminology.
mental-models.md — the engineering ideas (queue, worker, durability, decoupling, pipelines, etc.).


a Worker uses blocking Redis commands, so it needs its own dedicated connection

Redis is incredibly fast, but standard Redis can only focus on one thing at a time.When BullMQ spins up a "Worker" to process background jobs, that worker stands in the Redis storage room and says:"I am going to stand here and freeze until a new job arrives. Do not ask me to do anything else."This is called a blocking command. If BullMQ shared your single, live Redis connection, your entire application would freeze up waiting for that worker! By giving BullMQ the config instead, it safely spins up separate, dedicated connections for every worker so nothing gets stuck.

The one thing that is the point here: the API never waits for the work. It queues and returns an id instantly — that's the async contract everything else rides on. (And yeah, trusting req.body blindly is a prod sin — validation is its own concept, we bolt it on later. Not smuggling it in now.)

We built a durable job queue where the API accepts work, drops it in Redis, and returns immediately. The worker picks it up asynchronously. If the worker dies, jobs survive in Redis and resume when it restarts. The key insight: a queue is external state (Redis), not in-memory.

**Week 2 recap.** The id you'd been handing out became a *handle*. `GET /jobs/:id` now asks Redis for the job and reports where it is — `waiting → active → completed` (with the result object) or `failed` (with the error string). The worker can fail on command via `throw`, which BullMQ catches and turns into queryable `failed` state. Unknown id → clean 404. Everything lives in Redis, so it survives restarts — same durability as Week 1, now carrying results and errors.
