import uvicorn

from notify.app import create_app

app = create_app()


@app.get("/")
def read_root():
    return {"service": "notification-fanout", "docs": "/docs"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
