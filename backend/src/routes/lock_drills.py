from fastapi import APIRouter,Depends
from src.config.database import db_session
from src.utils.security import current,writer
from src.schemas.lock_drill import LockDrillInput
from src.services import lock_drills as service
router=APIRouter()
@router.get('/lock_drills')
def listing(db=Depends(db_session),user=Depends(current)):return service.listing(db)
@router.post('/lock_drills',status_code=201)
def create(data:LockDrillInput,db=Depends(db_session),user=Depends(writer)):return service.create(db,data,user)
from fastapi.responses import Response
import json
@router.get('/lock_drills/{ident}')
def detail(ident:int,db=Depends(db_session),user=Depends(current)):return service.detail(db,ident)
@router.get('/lock_drills/{ident}/export')
def export(ident:int,db=Depends(db_session),user=Depends(current)):
    return Response(json.dumps(service.detail(db,ident),ensure_ascii=False,indent=2),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="lock-{ident}.json"'})
