import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";

// Custom metrics
const taskScheduleRate = new Rate("task_schedule_success_rate");
const taskScheduleDuration = new Trend("task_schedule_duration");
const taskStatusRate = new Rate("task_status_success_rate");
const totalTasksScheduled = new Counter("total_tasks_scheduled");

// Test configuration
export const options = {
    stages: [
        { duration: "30s", target: 10 }, // Ramp up to 10 users
        { duration: "1m", target: 50 }, // Ramp up to 50 users
        { duration: "2m", target: 100 }, // Ramp up to 100 users
        { duration: "1m", target: 100 }, // Stay at 100 users
        { duration: "30s", target: 0 }, // Ramp down to 0 users
    ],
    thresholds: {
        http_req_duration: ["p(95)<500"], // 95% of requests must complete below 500ms
        http_req_failed: ["rate<0.01"], // Error rate must be less than 1%
        task_schedule_success_rate: ["rate>0.99"], // 99% success rate
    },
};

const BASE_URL = "http://localhost:8081";

// Store scheduled task IDs for status checking
const scheduledTasks = [];

export default function () {
    // Schedule a task
    const scheduledTime = new Date(Date.now() + 60000).toISOString(); // 1 minute from now

    const payload = JSON.stringify({
        endpoint: "https://httpbin.org/post",
        scheduled_at: scheduledTime,
        method: "POST",
        bearer_token: "test-token-123",
        payload: { test: "data", timestamp: Date.now() },
    });

    const params = {
        headers: {
            "Content-Type": "application/json",
        },
    };

    // Schedule task
    const scheduleRes = http.post(`${BASE_URL}/schedule`, payload, params);

    const scheduleSuccess = check(scheduleRes, {
        "schedule: status is 200": (r) => r.status === 200,
        "schedule: has task_id": (r) => {
            try {
                const body = JSON.parse(r.body);
                return body.task_id !== undefined;
            } catch (e) {
                return false;
            }
        },
    });

    taskScheduleRate.add(scheduleSuccess);
    taskScheduleDuration.add(scheduleRes.timings.duration);
    totalTasksScheduled.add(1);

    // Extract task ID for status checking
    if (scheduleRes.status === 200) {
        try {
            const body = JSON.parse(scheduleRes.body);
            scheduledTasks.push(body.task_id);
        } catch (e) {
            console.error("Failed to parse schedule response:", e);
        }
    }

    sleep(1);

    // Check task status (if we have scheduled tasks)
    if (scheduledTasks.length > 0) {
        const randomTaskId =
            scheduledTasks[Math.floor(Math.random() * scheduledTasks.length)];
        const statusRes = http.get(
            `${BASE_URL}/status?task_id=${randomTaskId}`,
            params
        );

        const statusSuccess = check(statusRes, {
            "status: response is 200": (r) => r.status === 200,
            "status: has task_id": (r) => {
                try {
                    const body = JSON.parse(r.body);
                    return body.task_id !== undefined;
                } catch (e) {
                    return false;
                }
            },
        });

        taskStatusRate.add(statusSuccess);
    }

    sleep(1);
}

export function handleSummary(data) {
    return {
        "summary.json": JSON.stringify(data),
        stdout: textSummary(data, { indent: " ", enableColors: true }),
    };
}

function textSummary(data, options) {
    const indent = options.indent || "";
    const enableColors = options.enableColors || false;

    let summary = "\n";
    summary += `${indent}┌─────────────────────────────────────────────────────────────┐\n`;
    summary += `${indent}│           Schedulo Performance Test Results               │\n`;
    summary += `${indent}└─────────────────────────────────────────────────────────────┘\n\n`;

    // HTTP metrics
    summary += `${indent}HTTP Metrics:\n`;
    summary += `${indent}  ✓ Requests:              ${data.metrics.http_reqs.values.count}\n`;
    summary += `${indent}  ✓ Request Rate:          ${data.metrics.http_reqs.values.rate.toFixed(2)} req/s\n`;
    summary += `${indent}  ✓ Request Duration (avg): ${data.metrics.http_req_duration.values.avg.toFixed(2)}ms\n`;
    summary += `${indent}  ✓ Request Duration (p95): ${data.metrics.http_req_duration.values["p(95)"].toFixed(2)}ms\n`;
    summary += `${indent}  ✓ Request Duration (p99): ${data.metrics.http_req_duration.values["p(99)"].toFixed(2)}ms\n`;
    summary += `${indent}  ✓ Failed Requests:       ${(data.metrics.http_req_failed.values.rate * 100).toFixed(2)}%\n\n`;

    // Custom metrics
    summary += `${indent}Schedulo Metrics:\n`;
    summary += `${indent}  ✓ Tasks Scheduled:       ${data.metrics.total_tasks_scheduled.values.count}\n`;
    summary += `${indent}  ✓ Schedule Success Rate: ${(data.metrics.task_schedule_success_rate.values.rate * 100).toFixed(2)}%\n`;
    summary += `${indent}  ✓ Schedule Duration (avg): ${data.metrics.task_schedule_duration.values.avg.toFixed(2)}ms\n`;
    summary += `${indent}  ✓ Status Check Success:  ${(data.metrics.task_status_success_rate.values.rate * 100).toFixed(2)}%\n\n`;

    return summary;
}
