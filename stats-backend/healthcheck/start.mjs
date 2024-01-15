import { TaskExecutor, ProposalFilters } from "@golem-sdk/golem-js"
import { PaymentService } from "@golem-sdk/golem-js"
PaymentService.prototype.acceptPayments = () => {}

const URL = "webserver:8002"

export async function submitStatus(status, taskId) {
    try {
        const endpoint = `http://${URL}/v2/healthcheck/status`

        const response = await fetch(endpoint, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                taskId: taskId,
                status: status,
            }),
        })

        if (response.ok) {
            return "Task status submitted successfully!"
        } else {
            const errorBody = await response.json()
            throw errorBody
        }
    } catch (error) {
        console.error("Error:", error)
    }
}

// Access command-line arguments
const whiteListIds = [process.argv[2]] // First argument as the whitelist ID
const networkKey = process.argv[3] // Second argument as the network key
const taskId = process.argv[4] // Second argument as the network key

;(async function main() {
    const executor = await TaskExecutor.create({
        package: "golem/alpine:latest",
        proposalFilter: ProposalFilters.whiteListProposalIdsFilter(whiteListIds),
        payment: {
            network: networkKey, // Use networkKey from the arguments
        },
        budget: "0.000001",
    })
    await submitStatus("Scanning the market for your provider...", taskId)

    try {
        await executor.run(async (ctx) => {
            await submitStatus("We found your provider. The task is now starting...", taskId)
            const result = (await ctx.run("echo -n $((2+2))")).stdout
            if (result == "4") {
                await submitStatus("Task completely successfully. The provider appears to be working as intended.", taskId)
            } else {
                await submitStatus("The task failed to compute.", taskId)
            }
        })
    } catch (err) {
        if (err == "Task 1 timeout") {
            await submitStatus(`We were unable to reach your provider, please make sure you're not already computing a task.`, taskId)
        } else {
            await submitStatus(`Error running task, reason: ${err}`, taskId)
        }
    } finally {
        await executor.shutdown()
    }
})()
