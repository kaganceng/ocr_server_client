import socket           # TCP soketleri için
import sys              # Komut satırı argümanları ve çıkış kodu

HOST = '127.0.0.1'
PORT = 4000

def send_pdf_and_receive_text(pdf_path: str):
    
    #PDF gönderip OCR sonucunu alan fonksiyon.
    
    
    try:
        #tcp bağlantısını oluştur
        with socket.create_connection((HOST, PORT)) as sock:
            try:
                #pdfyi oku ve gönder
                with open(pdf_path, 'rb') as f:
                    sock.sendall(f.read())
                #yazmayı bitirdiğimizi bildir
                sock.shutdown(socket.SHUT_WR)
            except FileNotFoundError:
                print(f"ERROR: Dosya bulunamadı: {pdf_path}")
                return
            except Exception as e:
                print(f"ERROR: PDF gönderilirken hata oluştu: {e}")
                return

            data = b''#sunucudan gelecek verıyı okumak ıcın bos dyte dizisi
            try:
                #sunucudan gelen yanıtı oku
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    data += chunk
            except Exception as e:
                print(f"ERROR: Sunucudan okunurken hata oluştu: {e}")
                return

        #okunan byteları metne çevir ve ekrana bas
        try:
            print(data.decode('utf-8'))
        except UnicodeDecodeError:
            print("ERROR: Gelen veriyi UTF-8 olarak çözümleyemedim.")
    except ConnectionRefusedError:
        print(f"ERROR: Bağlanılmak istenen {HOST}:{PORT} adresine ulaşılamıyor.")
    except socket.timeout:
        print("ERROR: Sunucuya bağlantı zaman aşımına uğradı.")
    except Exception as e:
        print(f"ERROR: Genel ağ hatası: {e}")

if __name__ == '__main__':
    #argüman kontrolü
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} pdf yolu olcak buraya da,2.pdf yolu")
        sys.exit(1)
    for pdf_path in sys.argv[1:]:
        print(f"\n=== Sending {pdf_path} ===")
        send_pdf_and_receive_text(pdf_path)

    
