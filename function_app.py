import azure.functions as func
import logging
import fitz  # PyMuPDF
import requests
import psycopg2
import json
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def chunk_text(text, chunk_size=1000, overlap=100):
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start += chunk_size - overlap
    return chunks

def get_embedding(text_chunk):
    url = os.environ["OPENAI_API_URL"]
    api_key = os.environ["OPENAI_API_KEY"]
    
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key
    }
    payload = {
        "input": [text_chunk]
    }
    
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
    data = response.json()
    return data['data'][0]['embedding']

# methods에 "GET"과 "POST"를 모두 허용하도록 수정
@app.route(route="process_pdf_pipeline", methods=["GET", "POST"])
def process_pdf_pipeline(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # 1. GET 요청인 경우: 웹 브라우저에서 접속했을 때 보여줄 HTML 업로드 화면
    if req.method == "GET":
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>증권사 리포트 PDF 업로드</title>
            <meta charset="utf-8">
            <style>
                body { font-family: Arial, sans-serif; padding: 20px; }
                .container { border: 1px solid #ccc; padding: 20px; border-radius: 8px; max-width: 500px; }
                input[type=text], input[type=file] { width: 100%; margin: 10px 0 20px 0; padding: 8px; }
                input[type=submit] { background-color: #4CAF50; color: white; padding: 10px 15px; border: none; border-radius: 4px; cursor: pointer; }
                input[type=submit]:hover { background-color: #45a049; }
            </style>
        </head>
        <body>
            <div class="container">
                <h2>📊 주식 종목 리포트 업로드 (PDF)</h2>
                <form action="/api/process_pdf_pipeline" method="post" enctype="multipart/form-data">
                    <label for="ticker"><b>주식 종목 (티커명):</b></label>
                    <input type="text" id="ticker" name="ticker" required placeholder="예: AAPL, TSLA">
                    
                    <label for="pdf_file"><b>PDF 파일 선택:</b></label>
                    <input type="file" id="pdf_file" name="pdf_file" accept=".pdf" required>
                    
                    <input type="submit" value="업로드 및 파이프라인 실행">
                </form>
            </div>
        </body>
        </html>
        """
        return func.HttpResponse(html_content, mimetype="text/html")


    # 2. POST 요청인 경우: 폼에서 업로드 버튼을 눌렀을 때 실행되는 파이프라인 로직
    try:
        pdf_file = req.files.get('pdf_file')
        ticker = req.form.get('ticker')

        if not pdf_file or not ticker:
            return func.HttpResponse("Please provide both 'pdf_file' and 'ticker'.", status_code=400)

        pdf_bytes = pdf_file.read()
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        
        full_text = ""
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            full_text += page.get_text()
            
        if not full_text.strip():
            return func.HttpResponse("Could not extract text from the provided PDF.", status_code=400)

        chunks = chunk_text(full_text)
        
        db_conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT"),
            dbname=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD")
        )
        cursor = db_conn.cursor()

        insert_query = """
            INSERT INTO VectorDBTable (StockTicker, VectorAnalysis)
            VALUES (%s, %s)
        """

        processed_chunks = 0
        for chunk in chunks:
            if len(chunk.strip()) < 10:
                continue
                
            vector_data = get_embedding(chunk)
            vector_str = json.dumps(vector_data)
            
            cursor.execute(insert_query, (ticker, vector_str))
            processed_chunks += 1

        db_conn.commit()
        cursor.close()
        db_conn.close()

        # 처리 완료 후 보여줄 결과 화면
        success_message = f"<h2>✅ 성공적으로 처리되었습니다!</h2>" \
                          f"<p>종목 '{ticker}'에 대한 PDF 파일이 추출 및 청킹되어 총 <b>{processed_chunks}</b>개의 벡터 데이터가 DB에 저장되었습니다.</p>" \
                          f"<a href='/api/process_pdf_pipeline'>돌아가기</a>"
        
        return func.HttpResponse(success_message, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.error(f"Error processing pipeline: {str(e)}")
        error_message = f"<h2>❌ 오류가 발생했습니다.</h2><p>{str(e)}</p><a href='/api/process_pdf_pipeline'>돌아가기</a>"
        return func.HttpResponse(error_message, mimetype="text/html", status_code=500)
