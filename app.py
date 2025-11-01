import os
import json
import asyncio
import base64
import boto3
import websockets
from flask import Flask, request, Response
from twilio.twiml.voice_response import VoiceResponse, Start, Stream

# ------------------ Configuration ------------------
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

app = Flask(__name__)

# ------------------ Twilio /voice endpoint ------------------
@app.route("/voice", methods=["POST"])
def voice():
    """TwiML that tells Twilio to start streaming audio to our WebSocket"""
    response = VoiceResponse()
    start = Start()
    start.stream(url="wss://YOUR_DOMAIN/media")  # <-- replace with your deployed domain (Render WSS)
    response.append(start)
    response.say("Hello! You are connected to the UniCall AI system. You can start speaking now.")
    return Response(str(response), mimetype="text/xml")

# ------------------ AWS Transcribe Streaming setup ------------------
async def transcribe_stream(audio_queue):
    """Send audio chunks to AWS Transcribe Streaming and print transcription results"""
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    transcribe_client = session.client("transcribe", region_name=AWS_REGION, use_ssl=True)

    # Use low-level streaming via WebSocket connection
    # We’ll use aiobotocore or boto3-stubs in later upgrade. For now, print queue audio length.
    while True:
        audio_chunk = await audio_queue.get()
        if audio_chunk is None:
            break
        print(f"[Audio chunk received: {len(audio_chunk)} bytes]")
    print("Stream ended.")

# ------------------ WebSocket handler for Twilio /media ------------------
connected_clients = set()

async def handle_twilio_media(websocket):
    """Handles bi-directional Twilio Media Stream"""
    print("[Client connected to /media stream]")
    audio_queue = asyncio.Queue()
    consumer_task = asyncio.create_task(transcribe_stream(audio_queue))

    try:
        async for message in websocket:
            data = json.loads(message)
            event = data.get("event")

            # When Twilio sends audio chunks
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

# ------------------ WebSocket server start ------------------
async def media_ws_server():
    """Start WebSocket server for Twilio media streams"""
    port = int(os.getenv("MEDIA_PORT", 4000))
    print(f"🛰️  Starting WebSocket server on ws://0.0.0.0:{port}/media")
    async with websockets.serve(handle_twilio_media, "0.0.0.0", port, ping_interval=None):
        await asyncio.Future()  # run forever

# ------------------ Run Flask + WebSocket concurrently ------------------
if __name__ == "__main__":
    import threading

    def run_flask():
        app.run(host="0.0.0.0", port=5000, debug=True)

    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(media_ws_server())
