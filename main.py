import uvicorn
from fastapi import FastAPI
from config import db
from controller import person


def init_app():
    db.init()
    
    app = FastAPI(
        title = "Lemoncode21 App",
        description = "CRUD sort pagination page",
        version = "1"
    )
    
    @app.on_event("startup")
    async def startup():
        await db.create_all()
    
    
    @app.on_event("shutdown")
    async def shutdown():
        await db.close()
    
    app.include_router(person.router)
    
    return app

app = init_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)