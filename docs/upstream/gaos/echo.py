import http.server
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        b=self.path.encode(); self.send_response(200); self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self,*a): pass
http.server.HTTPServer(("127.0.0.1",19556),H).serve_forever()
