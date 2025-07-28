import asyncio
import pytesseract
from pdf2image import convert_from_bytes
from concurrent.futures import ThreadPoolExecutor
import os
import io
import cv2
import numpy as np

from utils.db_utils import parse_attributes, save_to_db

# Poppler bin klasörünüzün tam yolu (içinde pdftoppm.exe var)
POPPLER_PATH = r"C:\Release-24.08.0-0\poppler-24.08.0\Library\bin"

#pil_image nesnesi hafızada piksel verisi tutar
def preprocess_image(pil_image):
    """
    PIL formatındaki bir görsele temel OCR ön işleme adımlarını uygular.
    """
    ocv_image = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)  # pilden ovc ye dönüştürme, pilin içindeki piksel verisini numpy dizisine çevirme rgb→bgr
    gray_image = cv2.cvtColor(ocv_image, cv2.COLOR_BGR2GRAY)

    _, binary_image = cv2.threshold(
        gray_image, 0, 255,
        cv2.THRESH_BINARY | cv2.THRESH_OTSU
    )

    return binary_image

def process_pdf_and_ocr(pdf_bytes):
    """
    Verilen PDF byte'larını işler, her sayfaya ön işleme uygular ve metni çıkarır.
    """
    print("OCR process started with preprocessing.")
    page_texts = []
    try:
        #CPU sayısını al none donerse 1 kullan
        cpu = os.cpu_count() or 1

        
        images = convert_from_bytes(
            pdf_bytes,
            dpi=200,#cozunurluk ayarı
            thread_count=cpu,
            poppler_path=POPPLER_PATH
        )
        print(f"PDF converted to {len(images)} pages.")

        custom_config = r'--oem 3 --psm 3 -c preserve_interword_spaces=1'
        print(f"Using Tesseract config: {custom_config}")

        for i, pil_image in enumerate(images, 1):
            print(f"  -> Processing page {i}/{len(images)}...")
            preprocessed_image = preprocess_image(pil_image)
            text = pytesseract.image_to_string(
                preprocessed_image, lang='eng', config=custom_config#ocr ile metni cikarma
            )
            page_texts.append(text)

        print("OCR process completed successfully.")
        return "\n\n==End of OCR for page==\n\n".join(page_texts)

    except Exception as e:
        error_message = f"ERROR in process_pdf_and_ocr: {str(e)}"
        print(error_message)
        return error_message

async def handle_client(reader, writer, executor):  # clientten pdf al, ocr çalıştır, sonucu geri yolla akışı
    print("🔍 DEBUG handle_client starting, using parse_attributes from:", parse_attributes.__module__)

    addr = writer.get_extra_info('peername')         # clientten id ve port bilgisini alır
    print(f"[+] New connection: {addr}")
    pdf_data = io.BytesIO()

    try:
        """asenkron şekilde en fazla 8 kb data okur; data boşsa client kapattı demektir
           döngü kırılır; doluysa gelen byteları bytesIO'ya ekler
        """
        while True:
            data = await reader.read(8192)
            if not data:
                break
            pdf_data.write(data)

        pdf_bytes = pdf_data.getvalue()  #biriken bütün byteları tek seferde alır binevi pdf'in bütün içeriği bizde demektir.

        if not pdf_bytes:  #0 byte geldiyse client yüksek ihtimalle yanlış istek attı
            print(f"Error: Received empty PDF data from {addr}.")
            writer.write(b"ERROR: Empty PDF data received.")
            await writer.drain()
            return

        print(f"PDF data received. Total: {len(pdf_bytes)} bytes. Starting OCR...")

        loop = asyncio.get_running_loop()
        timeout_seconds = 60 + (len(pdf_bytes) // (1024 * 1024)) * 60

        extracted_text = await asyncio.wait_for(
            loop.run_in_executor(executor, process_pdf_and_ocr, pdf_bytes),
            timeout=timeout_seconds
        )
        print("OCR process completed successfully.")

        
        #metni parse ederek alan sözlüğünü alma
        print("🔍 DEBUG about to call parse_attributes")
        record = parse_attributes(extracted_text)
        print("DEBUG Parsed record:", record)

        #raw metinden bir snippet oluştur ve yazdırma
        snippet = extracted_text.replace("\n", " ")[:100]
        print("DEBUG raw_text snippet:", snippet, "…")

        #dbye kaydetme
        save_to_db(record, extracted_text)
        print("DEBUG save_to_db() call done.")
        # ────────────────────────────────────────────────

        #son olarak metni clienta geri gönderme
        print(f"Sending result back to {addr}...")
        
        writer.write(extracted_text.encode('utf-8'))  #byte trafiğine metni ekler
        await writer.drain()                           #buffer boşalana kadar bekletir
        print(f"Result sent successfully. Process completed for {addr}.")

    except asyncio.TimeoutError:
        error_msg = b"ERROR: OCR process timed out on the server."
        print(error_msg.decode())
        writer.write(error_msg)
        await writer.drain()
    except Exception as e:
        error_msg = f"ERROR: An unexpected server error occurred: {e}"
        print(error_msg)
        writer.write(error_msg.encode('utf-8'))
        await writer.drain()
    finally:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        print(f"Connection with {addr} closed.")

async def start_server():
    cpu = os.cpu_count() or 1
    max_workers = max(1, cpu - 1)
    executor = ThreadPoolExecutor(max_workers=max_workers)
    print(f"ThreadPoolExecutor with {max_workers} workers started.")

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, executor),
        '127.0.0.1',
        4000
    )
    addr = server.sockets[0].getsockname()
    print(f"[*] Server listening on {addr}")

    async with server:
        try:
            await server.serve_forever()
        except KeyboardInterrupt:
            print("\n[*] Server is shutting down...")
        finally:
            executor.shutdown(wait=True)

if __name__ == "__main__":
    try:
        print(f"Tesseract version: {pytesseract.get_tesseract_version()}")
    except pytesseract.TesseractNotFoundError:
        print("CRITICAL ERROR: Tesseract is not installed or not in your PATH.")

    asyncio.run(start_server())
