# GPU Telemetry Pipeline AI Assistance and Human Review

## How I used AI

I used Codex to help design, implement, test, and document the GPU telemetry pipeline. The starting inputs were the project specification, `GPU Telemetry Pipeline Message Queue.pdf`, and the sample file, `dcgm_metrics_20250718_134233.csv`. I also required an open-source database and kept the work in a personal repository, without Splunk or AppDynamics tools or integrations.

AI helped in generating the implementation and supporting material. I interacted and questioned AI as well during the design phase. Some follow-up prompts changed the solution substantially. The first proposal was more elaborate than this interview exercise needed, and the later implementation also needed cleanup around configuration, documentation, and test coverage.

## What the prompts covered

| Stage | Work requested |
| --- | --- |
| Requirements and design | Read the specification and CSV, compare queue approaches, simplify the architecture, define subscriber capacity, and handle counter exhaustion. |
| Initial implementation | Build the pipeline, start with one instance and one collector slot |
| Scope and usability corrections | Remove unnecessary security setup, improve Helm and observability instructions, and remove payload hashing. |
| Code review and refactoring | Review code around collector lifecycle, diagnostics, time parsing, then improve configuration ownership, and documentation. |
| Deployment and testing | Keep Kind and Helm as the single deployment workflow, and raise unit coverage above 90 percent. |
| Final documentation | Explain configurations, metrics and make commands in README |

## Where human intervention changed the result

### Reducing the initial design to the required scope

The initial planning prompt asked the AI to read the requirements carefully and avoid inventing new ones. Even so, the first proposal included durable queue storage, completion receipts, an additional timestamp, and discussion of replicated brokers. Those mechanisms increased the amount of work and made the basic queue protocol harder to explain.

I challenged the need for durability based on requirement, an event ID, the extra timestamp, and a pull-based collector. I also specified that the timestamp should be assigned when the streamer processes the row and asked to start with one queue instance and multiple streamers and collectors.

The revised design uses a bounded in-memory queue, producer push, and collector subscriptions through which the queue pushes deliveries. PostgreSQL stores collected telemetry. It is an open-source database and supports the required per-GPU chronological queries with an index; a time-series database was not necessary for those queries.

### Separating queue acceptance from database persistence

I asked whether the queue should acknowledge the streamer as soon as it accepts a message, rather than waiting for the collector.

The earlier explanation did not make the two boundaries clear enough. The revised protocol has a producer acceptance response after the queue takes responsibility for the message in memory, and a separate collector acknowledgment after a successful database commit. The streamer retries an uncertain publication with the same key. The queue manages redelivery of unfinished assignments.

This clarification matters because a queue acceptance response must not imply that the row is already durable in PostgreSQL.

### Making push delivery safe for slow collectors

I asked how the queue would avoid overwhelming a slow collector and whether it needed to track current and maximum outstanding deliveries. The earlier design had an idle/busy concept, but the relationship to buffering and scheduling was not explicit enough.

The resulting design tracks each subscription's slot limit and current assignments, then dispatches round-robin among eligible subscribers. Each collector has one processing worker. A larger slot count allows bounded prefetch, it does not create more workers. That distinction was preserved in the implementation prompt and later documentation.

I also asked for an explicit sequence-overflow guard in a prompt. The design now checks before incrementing a signed 64-bit counter and reports exhaustion rather than wrapping. This is an unlikely operational boundary, but the behavior is defined and tested.

### Narrowing the interpretation of production readiness

My prompt asked for implementation with production-style care. That wording was broad. The resulting security and startup setup included TLS, certificates, authentication, database roles, and multiple passwords and connection URLs. These were beyond the implementation scope I wanted for the assignment.

I explicitly excluded HTTPS, gRPC TLS or mTLS, database TLS, authentication, and certificate management from the implementation. I asked to keep them as production follow-up work and simplify the code, tests, deployment, and installation instructions.

### Removing mechanisms without a clear requirement

I questioned `payload_hash` based on requirement. It could detect reuse of the same transport key with different content, but this additional check was not needed for the agreed retry behavior. It was later removed. The queue and database retain the first accepted value for a stable key without comparing payload contents.

### Improving code that worked but was difficult to follow

During the review, I questioned why every service loaded the same configuration, why collector session handling was so involved, why optional time filters used double pointers, and why HTTP instrumentation differed across services.

The review identified concrete problems alongside readability improvements. An unrelated invalid environment variable could prevent a service from starting. A useful receive error could be obscured by `context.Canceled`. The design document still contained a conflicting-payload claim after hashing had been removed, and its descriptions of the queue's data structures did not fully match the implementation.

The follow-up introduced service-specific configuration loaders, clearer collector subscription and send helpers, preserved cancellation causes, explicit optional-time parsing, consistent HTTP instrumentation, and corrected broker documentation. It retained the existing delivery protocol and resource safeguards. Focused tests were added for the behavior affected by the refactor.

### Requesting useful documentation rather than a list of features

Several prompts addressed gaps that only became obvious when I started following the README to run the system. It clarified that migration is a setup command or Kubernetes Job, rather than an additional long-running application. It questioned duplicated connection settings, working-directory assumptions, kubeconfig paths, and how to reach and interpret metrics.

The later documentation review found that the README was too complicated, configuration terms such as “slots” needed definitions, and the design read like instructions rather than an architecture description. My prompt requested a simpler guide, a complete service and configuration reference. The AI produced these revisions after my feedback.

### Setting a measurable test target

My earlier implementation measured unit coverage at about 71 percent. In a prompt, I required coverage above 90 percent and requested additional test cases. This threshold was my additional requirement, not a number specified in the project question.

The follow-up added coverage for database and migration behavior, lifecycle and cancellation failures, stream admission, producer recovery, queue error paths, and incomplete HTTP responses. The package scope remained unchanged, and a strict threshold check was added to the Make workflow. The latest recorded schema-cleanup run reports 97.7 percent statement coverage, or 850 of 870 statements.

Coverage was treated as one check alongside race detection, static analysis, builds, real PostgreSQL integration, and deployment smoke testing.

## What I would change in the initial prompts

I would state the implementation boundary earlier: one in-memory queue, PostgreSQL persistence at the collector, push subscriptions, one worker per collector, bounded configurable prefetch, and Kind with Helm as the deployment workflow. I would ask the AI to separate explicit requirements from optional production work before proposing mechanisms.

I would also request service-specific configuration, a concise operator guide, and a concrete test plan at the start. Any coverage threshold would be explicit. Protocol descriptions would have to state exactly when a producer gets acceptance, when a collector acknowledges, and what a process restart can lose.

The most useful follow-up prompts were specific. Asking what requirement justified `payload_hash` led to a clear simplification. Asking how a slow collector was protected led to explicit slot accounting. Requesting an assessment before allowing edits made the review easier to control. Broad requests helped create a first draft, but these narrower questions shaped the version I could understand and explain.

## Appendix Exact recorded implementation prompts

The following entries reproduce the saved submitted wording.

### Prompt 001 GPU Telemetry Pipeline Planning

```text
Plan a solution for this project question.

- Read each line of the GPU Telemetry Pipeline Message Queue.pdf document attached properly. You can use the dcgm_metrics_20250718_134233.csv to understand the format of the telemetry data.
- Think of all non functional requirements as well like scalability, availability etc.
- Return the plan in the following format: Design, Coding, Tests, Deployment, Observability
- Form the list of functional requirements mentioned in the doc and then form a solution around it.
- DO NOT create requirements of your own and do not hallucinate.
- Create a detailed design of each service.
- These are some of the requirements I have created, consider them when you are evaluating and check for their correctness.
  - Important functional requirements of a message queue:
    - Enqueue message
    - Dequeue message
    - Thread safe
    - Multiple producers, multiple consumer but the element should be processed only once and then deleted
    - Check if leasing concept is required (will be required if implemented as a separate service), library will be used by each instance of telemetry collector service.
    - Communication mechanism if service. Otherwise HTTP input and normal method call for popping from library.
    Non-functional:
    - Scalable optimise for high throughput
    - Available
    - Back pressure when too much load
    - Performant
- The solution will be created in a new repo. I have already created the repo and the chat session is happening in the repo's cloned folder. Create a plan to bootstrap the repo based on the document and the design.
- Ask clarifying questions only if necessary.
- DO NOT use Splunk or AppD skills or mcp or any tools
- Create a separate doc to capture your suggested design
```

### Prompt 002 Simplify the Telemetry Design

```text
- Why new timestamp field, no need to preserve existing timestamp field.
- Why new concept of event id?
- Why telemetry persistence, not part of requirement.
- Timestamp generation should happen at the time streamer processes it rather than collector
- An open-source database has to be used by collector. Should the database be a time-series database based on the queries or Postgres is fine with index on (timestamp, uuid) as the queries are simple?
- Should the telemetry collector follow a pull model? Shouldn’t it be a push model from queue side after the collector subscribes?
- And similarly shouldn’t be a push model from streamer to queue?
- Is durability a requirement? Can’t it just be in-memory queue service?
- How sequence number is being created at the queue side, how would you know if streamer is sending the same message or as retry?
- Let’s start with one instance of queue and multiple instances of streamer and collector.
- Do not complicate the design beyond the requirements.
```

### Prompt 003 Subscriber Flow Control and Scheduling

```text
In push model, a slow collector can easily get overwhelmed by the fast queue so the queue has to maintain collector state also right? Like slot size, current outstanding, maximum outstanding? and the messages among different collector subscribers has be distributed in some manner like round robin?
```

### Prompt 004 Bootstrap the Initial Production System

```text
- Start the bootstrap implementation and consider this a production system. Build it like that.
- Start with 1 instance of all service, we will go incrementally
- Start with 1 as slot size for collectors also and make it configurable. Consider each collector will have only 1 worker but still can have a higher slot size based on channel implementation so make it configurable.
- DO NOT COMMIT ANYTHING YET.
- DO NOT USE ANY SPLUNK OR APPD SKILLS OR TOOLS.
```

### Prompt 005 Simplify Security Scope

```text
- HTTPS, gRPC TLS/mTLS, database TLS, authentication, or certificate management are not required to be implemented for this project
- Capture these concepts as TODO for production implementation in the docs.
- Simplify the bootstrap implementation after removing these additional flows
- Adjust the tests and deployment configs also based on this
```

### Prompt 006 Payload Hash Rationale

```text
What is the use of payload_hash based on the requirement? Remove if it's not part of requirement.
```

### Prompt 007 Review Implementation

```text
Review the code and suggest changes if required based on requirement.
DO NOT MAKE ANY CHANGES NOW. JUST ASSESS.
```

### Prompt 008 Implement Reviewed Simplifications

```text
- Go ahead with the changes. Ensure no functionality is broken
- Adjust the test cases accordingly
```

### Prompt 009 Raise Unit Coverage Above 90 Percent

```text
I see the total test coverage is around 71%, it has to be above 90%. please add appropriate test cases.
```

### Prompt 010 Create a Simplified README

```text
README seems to be a little complicated:

- Create a separate simplified README2.md which I would like to check and then commit.
- Include a general intro to the tool
- Explain the services not in detail (details can be checked in the design doc)
- Then importantly the deployment steps
- Logs and troubleshooting steps
- The metrics which are available to get the insights about services
- Port forwarding steps
- APIs which are available for telemetry and their purpose (It can point to api spec)
- The sections may not be in the same order as I wrote above. Select a logical order.
- If you think something which is important is missing please ask.
```

The AI asked whether to include testing and cleanup. My reply was:

```text
Yes and also explain various commands of makefile
```