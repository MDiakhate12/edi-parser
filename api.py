from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import json
from pathlib import Path

app = FastAPI()

# simulation = 126
# env = "prod"

@app.get("/")
async def get_root():
    return JSONResponse(content={"status": "200 OK"})

@app.get("/onboard_data")
async def get_onboard_data(simulation: int, env: str):
    # Path to your JSON file
    json_file_path = Path(f"edi_parsing_v2/test_output_data/simulation_{simulation}_{env}/OnBoard.json")
    if not json_file_path.exists():
        raise HTTPException(status_code=404, detail="OnBoard.json file not found")
    
    try:
        with open(json_file_path, "r") as file:
            onboard_data = json.load(file)
        return JSONResponse(content=onboard_data)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding JSON file")

@app.get("/loadlist_data")
async def get_onboard_data(simulation: int, env: str):
    # Path to your JSON file
    json_file_path = Path(f"edi_parsing_v2/test_output_data/simulation_{simulation}_{env}/LoadList.json")
    if not json_file_path.exists():
        raise HTTPException(status_code=404, detail="OnBoard.json file not found")
    
    try:
        with open(json_file_path, "r") as file:
            onboard_data = json.load(file)
        return JSONResponse(content=onboard_data)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding JSON file")

@app.get("/size_and_type_codes")
async def get_size_and_type_codes():
    # Path to your JSON file
    json_file_path = Path(f"data/referential/size_and_type_codes.json")
    
    if not json_file_path.exists():
        raise HTTPException(status_code=404, detail="size_and_types_codes.json file not found")
    
    try:
        with open(json_file_path, "r") as file:
            onboard_data = json.load(file)
        return JSONResponse(content=onboard_data)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding JSON file")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
