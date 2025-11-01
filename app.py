import os
import json
import asyncio
import base64
import boto3
import websockets
from flask import Flask, request, Response
from twilio.twiml.voice_response import VoiceResponse, Start

# ------------------ Configuration ------------------
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

app = Flask(__name__)

# ------------------ Twilio /voice endpoint ------------------
@app.route("/voice", methods=["POST"])
def voice():
    """Return TwiML that instructs Twilio to start streaming audio"""
    response = VoiceResponse()
    start = Start()
    # Twilio Media Stream connects to /media websocket endpoint
    # (Render uses same domain, so only change to wss://your-domain/media)
    domain = os.getenv("RENDER_EXTERNAL_URL", "https://localhost")
    ws_url = domain.replace("https://", "wss://") + "/media"
    start.stream(url=ws_url)
    response.append(start)
    response.say("Hello! You are connected to the UniCall AI system. You can start speaking now.")
    return Response(str(response), mimetype="text/xml")

# ------------------ AWS Transcribe Streaming setup ------------------
async def transcribe_stream(audio_queue):
    """(placeholder) send audio chunks to AWS Transcribe Streaming"""
    while True:
        audio_chunk = await audio_queue.get()
        if audio_chunk is None:
            break
        print(f"[Audio chunk received: {len(audio_chunk)} bytes]")
    print("Stream ended.")

# ------------------ WebSocket handler for Twilio /media ------------------
async def handle_twilio_media(websocket):
    print("[Client connected to /media stream]")
    audio_queue = asyncio.Queue()
    consumer_task = asyncio.create_task(transcribe_stream(audio_queue))

    try:
        async for message in websocket:
            data = json.loads(message)
            event = data.get("event")

            if event == "media":
                payload = data["media"]["payload"]
                audio = base64.b64decode(payload)
                await audio_queue.put(audio)

            elif event == "start":
                print(f"[Stream started] Call SID: {data['start']['callSid']}")

            elif event == "stop":
                print("[Stream stopped]")
                break

    except Exception as e:
        print(f"[Error in media handler]: {e}")

    finally:
        await audio_queue.put(None)
        await consumer_task
        print("[Client disconnected from /media]")

# ------------------ Run Flask + WebSocket together on one port ------------------
async def main():
    port = int(os.getenv("PORT", 10000))  # Render provides PORT dynamically
    print(f"🚀 Starting on port {port}")
    async with websockets.serve(handle_twilio_media, "0.0.0.0", port, ping_interval=None):
        # Run Flask in a thread inside same process
        import threading
        def run_flask():
            app.run(host="0.0.0.0", port=port)
        threading.Thread(target=run_flask, daemon=True).start()

        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
