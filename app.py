import os, json, asyncio, base64, boto3
from quart import Quart, request, websocket
from twilio.twiml.voice_response import VoiceResponse, Start

# ---------- CONFIG ----------
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

app = Quart(__name__)

# ---------- ROUTE: Twilio /voice ----------
@app.post("/voice")
async def voice():
    """Twilio webhook to start media stream"""
    resp = VoiceResponse()
    start = Start()
    domain = os.getenv("RENDER_EXTERNAL_URL", "https://localhost")
    ws_url = domain.replace("https://", "wss://") + "/media"
    start.stream(url=ws_url)
    resp.append(start)
    resp.say("Hello! You are connected to UniCall AI. Start speaking now.")
    return str(resp), 200, {"Content-Type": "text/xml"}


@app.get("/")
async def home():
    return "🚀 UniCall AI is running!", 200


# ---------- ASYNC TASK: AWS Transcribe placeholder ----------
async def transcribe_stream(audio_queue: asyncio.Queue):
    """Simulates processing of audio stream."""
    while True:
        audio_chunk = await audio_queue.get()
        if audio_chunk is None:
            break
        print(f"[Audio chunk received: {len(audio_chunk)} bytes]")
    print("Stream ended.")


# ---------- WEBSOCKET HANDLER ----------
@app.websocket("/media")
async def handle_twilio_media():
    print("[Twilio WebSocket connected]")
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
        print("❌ WebSocket error:", e)
    finally:
        await audio_queue.put(None)
        await consumer_task
        print("[Twilio WebSocket disconnected]")


# ---------- ENTRY POINT ----------
if __name__ == "__main__":
    import hypercorn.asyncio
    from hypercorn.config import Config

    config = Config()
    config.bind = [f"0.0.0.0:{os.getenv('PORT', '10000')}"]
    config.use_reloader = False  # Avoid double startup on Render

    print("🚀 Starting UniCall AI (Quart + Hypercorn)")

    asyncio.run(hypercorn.asyncio.serve(app, config))


from twilio.rest import Client

@app.post("/trigger-call")
async def trigger_call():
    data = await request.get_json()
    to_number = data.get("to")
    if not to_number:
        return {"error": "Missing 'to' number"}, 400

    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    from_number = os.getenv("TWILIO_PHONE_NUMBER")
    domain = os.getenv("RENDER_EXTERNAL_URL")

    if not all([account_sid, auth_token, from_number, domain]):
        return {"error": "Twilio environment vars not set"}, 500

    client = Client(account_sid, auth_token)
    call = client.calls.create(
        to=to_number,
        from_=from_number,
        url=f"{domain}/voice"  # Twilio will fetch this TwiML
    )

    return {"status": "calling", "sid": call.sid}, 200
