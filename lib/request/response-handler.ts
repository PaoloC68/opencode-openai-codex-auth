import { logRequest, LOGGING_ENABLED } from "../logger.js";
import type { SSEEventData } from "../types.js";

interface SseParseResult {
	finalResponse?: unknown;
	lastResponseLike?: unknown;
	lastEvent?: unknown;
}

/**
 * Parse SSE stream to extract the final response.
 *
 * Tolerates "data:" with or without a trailing space, JSON split across
 * several data lines, and malformed events interleaved with valid ones.
 *
 * @param sseText - Complete SSE stream text
 * @returns The response.done payload, plus last-seen fallbacks
 */
function parseSseStream(sseText: string): SseParseResult {
	const lines = sseText.split(/\r?\n/);
	let pendingData: string[] = [];
	let finalResponse: unknown;
	let lastResponseLike: unknown;
	let lastEvent: unknown;

	const processEvent = (parsed: SSEEventData) => {
		lastEvent = parsed;
		const event = parsed as { type?: string; response?: unknown };
		if (!event || typeof event !== "object" || !("response" in event)) return;

		if (event.response !== undefined) {
			lastResponseLike = event.response;
		}
		if (event.type === "response.done" || event.type === "response.completed") {
			finalResponse = event.response;
		}
	};

	const tryFlush = (): boolean => {
		if (pendingData.length === 0) return false;
		try {
			const parsed = JSON.parse(pendingData.join("\n")) as SSEEventData;
			pendingData = [];
			processEvent(parsed);
			return true;
		} catch {
			return false;
		}
	};

	for (const line of lines) {
		if (line === "") {
			tryFlush();
			pendingData = [];
			continue;
		}
		if (!line.startsWith("data:")) continue;

		const content = line.replace(/^data:\s?/, "");
		pendingData.push(content);
		if (tryFlush()) continue;

		// The accumulated lines did not parse together, so retry this line alone:
		// otherwise one malformed event poisons every event that follows it.
		try {
			const parsed = JSON.parse(content) as SSEEventData;
			pendingData = [];
			processEvent(parsed);
		} catch {
			// Keep accumulating - the JSON may span several data lines.
		}
	}

	tryFlush();

	return { finalResponse, lastResponseLike, lastEvent };
}

/**
 * Convert SSE stream response to JSON for generateText()
 * @param response - Fetch response with SSE stream
 * @param headers - Response headers
 * @returns Response with JSON body
 */
export async function convertSseToJson(response: Response, headers: Headers): Promise<Response> {
	if (!response.body) {
		throw new Error('[openai-codex-plugin] Response has no body');
	}
	const reader = response.body.getReader();
	const decoder = new TextDecoder();
	let fullText = '';

	try {
		// Consume the entire stream
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			fullText += decoder.decode(value, { stream: true });
		}

		if (LOGGING_ENABLED) {
			logRequest("stream-full", { fullContent: fullText });
		}

		// Parse SSE events to extract the final response
		const parsed = parseSseStream(fullText);
		const responsePayload =
			parsed.finalResponse ?? parsed.lastResponseLike ?? parsed.lastEvent;

		if (!responsePayload) {
			console.error('[openai-codex-plugin] Could not find JSON in SSE stream');
			logRequest("stream-error", { error: "No JSON events found in SSE stream" });

			// Return original stream if we can't parse
			return new Response(fullText, {
				status: response.status,
				statusText: response.statusText,
				headers: headers,
			});
		}

		// Return as plain JSON (not SSE)
		const jsonHeaders = new Headers(headers);
		jsonHeaders.set('content-type', 'application/json; charset=utf-8');

		if (!parsed.finalResponse) {
			logRequest("stream-warning", {
				warning: "No final response event; using last JSON event",
			});
		}

		return new Response(JSON.stringify(responsePayload), {
			status: response.status,
			statusText: response.statusText,
			headers: jsonHeaders,
		});

	} catch (error) {
		console.error('[openai-codex-plugin] Error converting stream:', error);
		logRequest("stream-error", { error: String(error) });
		throw error;
	}
}

/**
 * Ensure response has content-type header
 * @param headers - Response headers
 * @returns Headers with content-type set
 */
export function ensureContentType(headers: Headers): Headers {
	const responseHeaders = new Headers(headers);

	if (!responseHeaders.has('content-type')) {
		responseHeaders.set('content-type', 'text/event-stream; charset=utf-8');
	}

	return responseHeaders;
}
