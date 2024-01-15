const URL = process.env.DOCKER === "true" ? "django:8002" : "api.localhost"

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
