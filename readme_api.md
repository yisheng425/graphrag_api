114服务器上
cd graphrag
conda activate graphrag
第一个终端
export GRAPHRAG_CELERY_BROKER_URL=redis://localhost:6379/1
export GRAPHRAG_CELERY_BACKEND_URL=redis://localhost:6379/1
nohup celery -A graphrag.service.tasks worker --pool solo --loglevel=debug >celery-0106-1541.log 2>&1 &


第二个终端
nohup uvicorn graphrag.service.app:app --host 0.0.0.0 --port 8001 >uvicorn-0106-1541.log 2>&1 &

第三个终端
查询

curl -X POST http://127.0.0.1:8001/query -H "Content-Type: application/json" -d '{"root": "/home/xhm/graphrag/ragtest", "method": "local", "query": "海尔"}'

curl -X POST http://127.0.0.1:8001/query -H "Content-Type: application/json" -d '{"root": "/home/xhm/graphrag/ragtest", "method": "local", "data":"/home/xhm/graphrag/ragtest/update_index_output/20251025-140433/delta","query": "海尔"}'

graphrag query --root ./ragtest --data ./ragtest/update_index_output/20251025-140433/delta --method local --query "海尔"


查询任务结果（用给的task_id）
curl http://127.0.0.1:8001/tasks/3152b608-9d64-43d1-9377-a3b3b324324b

8bded4e4-9c34-44f4-8499-105ef9a290c5

  初次构建索引
   curl -X POST http://127.0.0.1:8001/index \
        -H "Content-Type: application/json" \
        -d '{"root":"/home/xhm/graphrag/ragtest"}'
    
  增量更新
   curl -X POST http://127.0.0.1:8001/update -H "Content-Type: application/json" -d '{"root":"/home/xhm/graphrag/ragtest"}'

