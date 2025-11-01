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

# Store active WebSocket connections
active_connections = set()

# ------------------ Twilio /voice endpoint ------------------
@app.route("/voice", methods=["POST"])
def voice():
    """Return TwiML that instructs Twilio to start streaming audio"""
    response = VoiceResponse()
    start = Start()
    domain = os.getenv("RENDER_EXTERNAL_URL", "https://localhost")
    # Use the same domain and path for WebSocket
    ws_url = domain.replace("https://", "wss://") + "/media"
    start.stream(url=ws_url)
    response.append(start)
    response.say("Hello! You are connected to the UniCall AI system. You can start speaking now.")
    return Response(str(response), mimetype="text/xml")

# Health check endpoint (required by Render)
@app.route("/", methods=["GET"])
@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

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
async def handle_twilio_media(websocket, path):
    """Handle WebSocket connections at /media path"""
    print(f"[Client connected to /media stream] Path: {path}")
    
    # Only handle /media path
    if path != "/media":
        await websocket.close(1003, "Invalid path")
        return
        
    active_connections.add(websocket)
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

    except websockets.exceptions.ConnectionClosed:
        print("[WebSocket connection closed]")
    except Exception as e:
        print(f"[Error in media handler]: {e}")
    finally:
        active_connections.discard(websocket)
        await audio_queue.put(None)
        await consumer_task
        print("[Client disconnected from /media]")

# ------------------ Combined Server Setup ------------------
def start_websocket_server():
    """Start WebSocket server in a separate thread"""
    port = int(os.getenv("PORT", 10000))
    
    async def server_main():
        print(f"🔌 Starting WebSocket server on port {port}")
        async with websockets.serve(
            handle_twilio_media, 
            "0.0.0.0", 
            port, 
            ping_interval=None,
            # Important for Render compatibility
            process_request=process_request
        ):
            await asyncio.Future()  # run forever
    
    # Run WebSocket server in current event loop
    asyncio.create_task(server_main())

async def process_request(path, request_headers):
    """Handle HTTP requests that come to WebSocket server"""
    if path == "/health" or path == "/":
        return (200, [], b"OK")
    return None

def run_flask():
    """Run Flask app - but don't actually start the server"""
    # Flask will be run by Render's built-in server
    pass

async def main():
    """Main async function"""
    port = int(os.getenv("PORT", 10000))
    print(f"🚀 Starting combined server on port {port}")
    
    # Start WebSocket server
    start_websocket_server()
    
    # Keep the event loop running
    await asyncio.Future()

# For Render deployment, we need to use their startup process
if __name__ == "__main__":
    # This will only run locally
    asyncio.run(main())
    