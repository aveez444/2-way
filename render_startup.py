import asyncio
import threading
import os
from app import app, main

def run_flask():
    """Run Flask app using Waitress for production"""
    from waitress import serve
    port = int(os.getenv("PORT", 10000))
    print(f"Starting Flask on port {port}")
    serve(app, host="0.0.0.0", port=port)

def run_combined():
    """Run both Flask and WebSocket together"""
    port = int(os.getenv("PORT", 10000))
    print(f"Starting combined server on port {port}")
    
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Run WebSocket server in main thread
    asyncio.run(main())

if __name__ == "__main__":
    run_combined()
    