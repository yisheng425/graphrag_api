114服务器上
cd graphrag
conda activate graphrag
第一个终端
export GRAPHRAG_CELERY_BROKER_URL=redis://localhost:6379/1
export GRAPHRAG_CELERY_BACKEND_URL=redis://localhost:6379/1
nohup celery -A graphrag.service.tasks worker --pool solo --loglevel=debug >celery-0108-21:46.log 2>&1 &

nohup celery -A graphrag.service.tasks worker --pool prefork --concurrency=1 --loglevel=info > celery-prefork-0108-night.log 2>&1 &



第二个终端
nohup uvicorn graphrag.service.app:app --host 0.0.0.0 --port 8001 >uvicorn-0106-1541.log 2>&1 &

nohup uvicorn graphrag.service.app:app --host 0.0.0.0 --port 8001 > uvicorn-0108-21:46.log 2>&1 &

第三个终端
查询

curl -X POST http://127.0.0.1:8001/query -H "Content-Type: application/json" -d '{"root": "/home/xhm/graphrag/ragtest", "method": "local", "query": "海尔"}'

curl -X POST http://127.0.0.1:8001/query -H "Content-Type: application/json" -d '{"root": "/home/xhm/graphrag/ragtest", "method": "local", "data":"/home/xhm/graphrag/ragtest/update_index_output/20251025-140433/delta","query": "海尔"}'

graphrag query --root ./ragtest --data ./ragtest/update_index_output/20251025-140433/delta --method local --query "海尔"


查询任务结果（用给的task_id）
curl http://127.0.0.1:8001/tasks/263c738d-a611-4c0c-a068-38078b94ed3a

8bded4e4-9c34-44f4-8499-105ef9a290c5

  初次构建索引
   curl -X POST http://127.0.0.1:8001/index \
        -H "Content-Type: application/json" \
        -d '{"root":"/home/xhm/graphrag/ragtest"}'
    
  增量更新
   curl -X POST http://127.0.0.1:8001/update -H "Content-Type: application/json" -d '{"root":"/home/xhm/graphrag/ragtest"}'


 方式 1：通过 redis-cli 直接查看队列长度                                                                                                           
                                                                                                                                                    
  redis-cli -n 1 LLEN celery         

 如果想清空队列（慎用）：                                                                                                                          
                                                                                                                                                    
  # 清空队列中所有待处理任务                                                                                                                                                                                                                                                                                                                                                                     
  # 或者直接用 redis                                                                                                                                
  redis-cli -n 1 DEL celery     