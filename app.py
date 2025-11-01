import os, json, asyncio, base64, boto3, websockets
from flask import Flask, request, Response
from twilio.twiml.voice_response import VoiceResponse, Start

# ---------- CONFIG ----------
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

app = Flask(__name__)

# ---------- FLASK: Twilio /voice ----------
@app.route("/voice", methods=["POST"])
def voice():
    """Return TwiML telling Twilio to open a media stream"""
    resp = VoiceResponse()
    start = Start()
    domain = os.getenv("RENDER_EXTERNAL_URL", "https://localhost")
    ws_url = domain.replace("https://", "wss://") + "/media"
    start.stream(url=ws_url)
    resp.append(start)
    resp.say("Hello! You are connected to UniCall AI. Start speaking now.")
    return Response(str(resp), mimetype="text/xml")

@app.route("/")
def home():
    return "UniCall AI running!", 200

# ---------- PLACEHOLDER: AWS Transcribe Streaming ----------
async def transcribe_stream(audio_queue):
    while True:
        audio_chunk = await audio_queue.get()
        if audio_chunk is None:
            break
        print(f"[Audio chunk received: {len(audio_chunk)} bytes]")
    print("Stream ended.")

# ---------- WEBSOCKET HANDLER ----------
async def handle_twilio_media(websocket, path):
    if path != "/media":
        await websocket.close(code=1003, reason="Invalid path")
        return

    print(f"[Client connected: {path}]")
    audio_queue = asyncio.Queue()
    consumer_task = asyncio.create_task(transcribe_stream(audio_queue))

    try:
        async for message in websocket:
            data = json.loads(message)
            event = data.get("event")
            if event == "media":
                audio = base64.b64decode(data["media"]["payload"])
                await audio_queue.put(audio)
            elif event == "start":
                print(f"[Stream started] Call SID: {data['start']['callSid']}")
            elif event == "stop":
                print("[Stream stopped]")
                break
    except Exception as e:
        print("WebSocket error:", e)
    finally:
        await audio_queue.put(None)
        await consumer_task
        print("[Client disconnected]")

# ---------- SINGLE PORT SERVER ----------
async def main():
    port = int(os.getenv("PORT", 10000))
    print(f"🚀 Flask + WebSocket running on port {port}")

    # Run Flask in a background thread
    import threading
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=port, threaded=True), daemon=True).start()

    # Start WebSocket on same port
    async with websockets.serve(
        handle_twilio_media,
        host="0.0.0.0",
        port=port,
        ping_interval=None,
    ):
        await asyncio.Future()  # keep running

if __name__ == "__main__":
    asyncio.run(main())
