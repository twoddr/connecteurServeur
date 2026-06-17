package ntw;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Serveur HTTP minimal pour le transfert de fichiers multipart.
 * 
 * Endpoints :
 * POST /api/documents/upload → Reçoit un fichier multipart, le stocke, retourne
 * le chemin
 * GET /api/documents/download/{filename} → Renvoie le fichier
 * DELETE /api/documents/delete/{filename} → Supprime le fichier
 * GET /api/documents/ping → Test de connexion (retourne "OK")
 * GET /api/documents/list → Liste les fichiers disponibles
 * 
 * Les fichiers sont stockés dans le dossier "fichiers/transfers".
 */
public class HttpFileServer {

    private static final String STORAGE_DIR = "fichiers/transfers";
    private static final int BUFFER_SIZE = 8192;

    private final int httpPort;
    private final ServeurLogger logger;
    private final ApplicationNtwServeur mainProgram;
    private ServerSocket serverSocket;
    private volatile boolean running;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    static {
        try {
            Files.createDirectories(Paths.get(STORAGE_DIR));
        } catch (IOException e) {
            System.err.println("❌ HttpFileServer: Impossible de créer " + STORAGE_DIR + " : " + e.getMessage());
        }
    }

    public HttpFileServer(int httpPort, ApplicationNtwServeur mainProgram, ServeurLogger logger) {
        this.httpPort = httpPort;
        this.mainProgram = mainProgram;
        this.logger = logger;
    }

    /**
     * Démarre le serveur HTTP dans un thread séparé.
     */
    public void start() {
        if (httpPort <= 0) {
            log("INFO", "HttpFileServer désactivé (port=" + httpPort + ")");
            return;
        }
        if (running) {
            log("WARN", "HttpFileServer déjà en cours d'exécution");
            return;
        }

        Thread httpThread = new Thread(() -> {
            try {
                serverSocket = new ServerSocket(httpPort, 50);
                running = true;
                log("INFO", "✅ Serveur HTTP démarré sur le port " + httpPort +
                        " (stockage: " + Paths.get(STORAGE_DIR).toAbsolutePath() + ")");

                while (running && !serverSocket.isClosed()) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        threadPool.submit(() -> traiterRequete(clientSocket));
                    } catch (IOException e) {
                        if (running) {
                            log("ERROR", "Erreur accept: " + e.getMessage());
                        }
                    }
                }
            } catch (IOException e) {
                log("ERROR", "Impossible d'ouvrir le port HTTP " + httpPort + " : " + e.getMessage());
            }
        });
        httpThread.setName("HttpFileServer-" + httpPort);
        httpThread.setDaemon(true);
        httpThread.start();
    }

    /**
     * Arrête le serveur HTTP.
     */
    public void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            log("ERROR", "Erreur arrêt serveur HTTP: " + e.getMessage());
        }
        threadPool.shutdown();
        log("INFO", "Serveur HTTP arrêté");
    }

    // ============================================================
    // TRAITEMENT DES REQUÊTES HTTP
    // ============================================================

    private void traiterRequete(Socket clientSocket) {
        try (clientSocket;
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream()) {

            // Lire la ligne de requête
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String requestLine = reader.readLine();
            if (requestLine == null || requestLine.isEmpty())
                return;

            String[] parts = requestLine.split(" ");
            if (parts.length < 2) {
                envoyerReponse(out, 400, "Bad Request", "Format de requête invalide");
                return;
            }

            String method = parts[0];
            String path = URLDecoder.decode(parts[1], StandardCharsets.UTF_8.name());

            // Lire les en-têtes
            Map<String, String> headers = new HashMap<>();
            String headerLine;
            int contentLength = 0;
            String contentType = null;
            while ((headerLine = reader.readLine()) != null && !headerLine.isEmpty()) {
                int colonIdx = headerLine.indexOf(':');
                if (colonIdx > 0) {
                    String key = headerLine.substring(0, colonIdx).trim().toLowerCase();
                    String value = headerLine.substring(colonIdx + 1).trim();
                    headers.put(key, value);
                    if (key.equals("content-length"))
                        contentLength = Integer.parseInt(value);
                    if (key.equals("content-type"))
                        contentType = value;
                }
            }

            // Lire le corps (si présent)
            byte[] body = null;
            if (contentLength > 0) {
                body = new byte[contentLength];
                int read = 0;
                while (read < contentLength) {
                    int n = in.read(body, read, contentLength - read);
                    if (n == -1)
                        break;
                    read += n;
                }
            }

            // Router la requête
            router(method, path, headers, body, out);

        } catch (Exception e) {
            log("ERROR", "Erreur traitement requête HTTP: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }
    }

    private void router(String method, String path, Map<String, String> headers,
            byte[] body, OutputStream out) throws IOException {
        // Nettoyer le path
        path = path.replaceAll("/+", "/");

        // GET /api/documents/ping
        if ("GET".equalsIgnoreCase(method) && "/api/documents/ping".equals(path)) {
            envoyerReponse(out, 200, "OK", "OK");
            return;
        }

        // GET /api/documents/list
        if ("GET".equalsIgnoreCase(method) && "/api/documents/list".equals(path)) {
            String listing = listerFichiersJson();
            envoyerReponse(out, 200, "OK", listing, "application/json");
            return;
        }

        // POST /api/documents/upload
        if ("POST".equalsIgnoreCase(method) && "/api/documents/upload".equals(path)) {
            traiterUpload(headers, body, out);
            return;
        }

        // GET /api/documents/download/{filename}
        if ("GET".equalsIgnoreCase(method) && path.startsWith("/api/documents/download/")) {
            String filename = path.substring("/api/documents/download/".length());
            traiterDownload(filename, out);
            return;
        }

        // DELETE /api/documents/delete/{filename}
        if ("DELETE".equalsIgnoreCase(method) && path.startsWith("/api/documents/delete/")) {
            String filename = path.substring("/api/documents/delete/".length());
            traiterDelete(filename, out);
            return;
        }

        // 404
        envoyerReponse(out, 404, "Not Found", "Endpoint inconnu: " + method + " " + path);
    }

    // ============================================================
    // UPLOAD (multipart) - parsing binaire correct
    // ============================================================

    private void traiterUpload(Map<String, String> headers, byte[] body, OutputStream out) throws IOException {
        String contentType = headers.get("content-type");
        if (contentType == null || !contentType.startsWith("multipart/form-data")) {
            envoyerReponse(out, 400, "Bad Request", "Content-Type doit être multipart/form-data");
            return;
        }

        // Extraire le boundary
        int boundaryIdx = contentType.indexOf("boundary=");
        if (boundaryIdx < 0) {
            envoyerReponse(out, 400, "Bad Request", "Boundary manquant dans Content-Type");
            return;
        }
        String boundaryStr = contentType.substring(boundaryIdx + 9).trim();
        if (boundaryStr.startsWith("\"") && boundaryStr.endsWith("\"")) {
            boundaryStr = boundaryStr.substring(1, boundaryStr.length() - 1);
        }
        byte[] boundaryBytes = ("--" + boundaryStr).getBytes(StandardCharsets.US_ASCII);
        byte[] headersEnd = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

        // Analyser le multipart en mode binaire
        String uploadedFilename = null;
        byte[] fileData = null;
        int pos = 0;

        while (pos < body.length) {
            // Chercher le prochain boundary
            int bStart = indexOf(body, boundaryBytes, pos);
            if (bStart < 0)
                break;

            int bEnd = bStart + boundaryBytes.length;

            // Vérifier si c'est le boundary final (avec "--" après)
            if (bEnd + 2 <= body.length && body[bEnd] == '-' && body[bEnd + 1] == '-') {
                break; // Boundary final
            }

            // Aller après le \r\n qui suit le boundary
            int partStart = skipLineEnd(body, bEnd);
            if (partStart < 0)
                break;

            // Chercher la fin des en-têtes (\r\n\r\n)
            int headerEndIdx = indexOf(body, headersEnd, partStart);
            if (headerEndIdx < 0)
                break;

            // Lire les en-têtes dans cette plage
            String headerStr = new String(body, partStart, headerEndIdx - partStart, StandardCharsets.UTF_8);

            // Extraire le nom de fichier original (si présent)
            int fnIdx = headerStr.indexOf("filename=\"");
            if (fnIdx >= 0) {
                int fnEnd = headerStr.indexOf("\"", fnIdx + 10);
                if (fnEnd > fnIdx) {
                    uploadedFilename = headerStr.substring(fnIdx + 10, fnEnd);
                }
            }

            // Le contenu commence après le \r\n\r\n
            int contentStart = headerEndIdx + 4;
            // Le contenu se termine au prochain boundary (ou à la fin)
            int nextB = indexOf(body, boundaryBytes, contentStart);
            int contentEnd = (nextB > 0) ? nextB : body.length;

            // Enlever le \r\n final avant le boundary
            if (contentEnd > 2 && body[contentEnd - 2] == '\r' && body[contentEnd - 1] == '\n') {
                contentEnd -= 2;
            }

            // Si c'est la première partie avec un filename, c'est notre fichier
            if (uploadedFilename != null) {
                fileData = new byte[contentEnd - contentStart];
                System.arraycopy(body, contentStart, fileData, 0, fileData.length);
                break; // On a trouvé notre fichier
            }

            // Sinon, continuer à chercher
            pos = contentEnd;
        }

        if (fileData == null || fileData.length == 0) {
            // Fallback: utiliser l'ancienne méthode String pour diagnostiquer
            log("WARN", "Aucune donnée fichier trouvée dans la requête multipart");
            envoyerReponse(out, 400, "Bad Request", "Aucune donnée fichier trouvée");
            return;
        }

        // Déterminer le nom du fichier
        String filename = sanitizeFilename(
                uploadedFilename != null ? uploadedFilename : "file_" + System.currentTimeMillis());

        // Éviter l'écrasement
        Path destPath = Paths.get(STORAGE_DIR, filename);
        if (Files.exists(destPath)) {
            String base = filename.contains(".") ? filename.substring(0, filename.lastIndexOf('.')) : filename;
            String ext = filename.contains(".") ? filename.substring(filename.lastIndexOf('.')) : "";
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            filename = base + "_" + timestamp + ext;
            destPath = Paths.get(STORAGE_DIR, filename);
        }

        Files.write(destPath, fileData);
        long size = Files.size(destPath);

        log("INFO", "Fichier reçu: " + filename + " (" + size + " octets)");

        // Répondre avec le chemin relatif
        String responsePath = "fichiers/transfers/" + filename;
        envoyerReponse(out, 200, "OK", responsePath, "text/plain");
    }

    /**
     * Recherche un tableau d'octets (needle) dans un autre (haystack) à partir de
     * startPos.
     */
    private static int indexOf(byte[] haystack, byte[] needle, int startPos) {
        outer: for (int i = startPos; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    /**
     * Saute les caractères \r\n après une position donnée.
     */
    private static int skipLineEnd(byte[] data, int pos) {
        while (pos < data.length && (data[pos] == '\r' || data[pos] == '\n')) {
            pos++;
        }
        return (pos < data.length) ? pos : -1;
    }

    // ============================================================
    // DOWNLOAD
    // ============================================================

    private void traiterDownload(String filename, OutputStream out) throws IOException {
        String safeName = sanitizeFilename(filename);

        Path filePath = Paths.get(STORAGE_DIR, safeName);
        if (!Files.exists(filePath) || Files.isDirectory(filePath)) {
            envoyerReponse(out, 404, "Not Found", "Fichier introuvable: " + safeName);
            return;
        }

        byte[] fileData = Files.readAllBytes(filePath);
        String contentType = devinerContentType(safeName);

        log("INFO", "📤 Fichier envoyé: " + safeName + " (" + fileData.length + " octets)");

        // Envoyer la réponse avec le fichier
        StringBuilder header = new StringBuilder();
        header.append("HTTP/1.1 200 OK\r\n");
        header.append("Content-Type: ").append(contentType).append("\r\n");
        header.append("Content-Length: ").append(fileData.length).append("\r\n");
        header.append("Content-Disposition: attachment; filename=\"").append(safeName).append("\"\r\n");
        header.append("Connection: close\r\n");
        header.append("\r\n");

        out.write(header.toString().getBytes(StandardCharsets.UTF_8));
        out.write(fileData);
        out.flush();
    }

    // ============================================================
    // DELETE
    // ============================================================

    private void traiterDelete(String filename, OutputStream out) throws IOException {
        String safeName = sanitizeFilename(filename);
        Path filePath = Paths.get(STORAGE_DIR, safeName);

        if (!Files.exists(filePath)) {
            envoyerReponse(out, 404, "Not Found", "Fichier introuvable: " + safeName);
            return;
        }

        Files.delete(filePath);
        log("INFO", "🗑️ Fichier supprimé: " + safeName);
        envoyerReponse(out, 200, "OK", "Fichier supprimé: " + safeName);
    }

    // ============================================================
    // UTILITAIRES
    // ============================================================

    private void envoyerReponse(OutputStream out, int code, String message, String body) throws IOException {
        envoyerReponse(out, code, message, body, "text/plain; charset=UTF-8");
    }

    private void envoyerReponse(OutputStream out, int code, String message,
            String body, String contentType) throws IOException {
        byte[] bodyBytes = body != null ? body.getBytes(StandardCharsets.UTF_8) : new byte[0];
        StringBuilder header = new StringBuilder();
        header.append("HTTP/1.1 ").append(code).append(" ").append(message).append("\r\n");
        header.append("Content-Type: ").append(contentType).append("\r\n");
        header.append("Content-Length: ").append(bodyBytes.length).append("\r\n");
        header.append("Connection: close\r\n");
        header.append("\r\n");
        out.write(header.toString().getBytes(StandardCharsets.UTF_8));
        if (bodyBytes.length > 0) {
            out.write(bodyBytes);
        }
        out.flush();
    }

    private String listerFichiersJson() {
        StringBuilder json = new StringBuilder("[");
        try {
            Files.list(Paths.get(STORAGE_DIR))
                    .filter(p -> !Files.isDirectory(p))
                    .forEach(p -> {
                        try {
                            if (json.length() > 1)
                                json.append(",");
                            json.append("{");
                            json.append("\"filename\":\"").append(escapeJson(p.getFileName().toString())).append("\",");
                            json.append("\"size\":").append(Files.size(p)).append(",");
                            json.append("\"lastModified\":\"")
                                    .append(Files.getLastModifiedTime(p).toString()).append("\"");
                            json.append("}");
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException e) {
            log("ERROR", "Erreur listage fichiers: " + e.getMessage());
        }
        json.append("]");
        return json.toString();
    }

    private static String sanitizeFilename(String filename) {
        if (filename == null)
            return "file.bin";
        String safe = filename.replaceAll("[/\\\\:;<>|?*\"']", "_");
        safe = safe.replace("..", "_");
        if (safe.isEmpty())
            safe = "file.bin";
        if (safe.length() > 255) {
            int dot = safe.lastIndexOf('.');
            if (dot > 0 && safe.length() - dot < 20) {
                safe = safe.substring(0, 235 - (safe.length() - dot)) + safe.substring(dot);
            } else {
                safe = safe.substring(0, 255);
            }
        }
        return safe;
    }

    private static String devinerContentType(String filename) {
        String ext = filename.contains(".") ? filename.substring(filename.lastIndexOf('.')).toLowerCase() : "";
        switch (ext) {
            case ".txt":
                return "text/plain";
            case ".html":
            case ".htm":
                return "text/html";
            case ".json":
                return "application/json";
            case ".pdf":
                return "application/pdf";
            case ".jpg":
            case ".jpeg":
                return "image/jpeg";
            case ".png":
                return "image/png";
            case ".gif":
                return "image/gif";
            case ".zip":
                return "application/zip";
            case ".csv":
                return "text/csv";
            case ".doc":
                return "application/msword";
            case ".docx":
                return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
            default:
                return "application/octet-stream";
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    private void log(String level, String msg) {
        if (logger != null && mainProgram != null) {
            if ("ERROR".equals(level))
                logger.error("HttpFileServer", msg);
            else if ("WARN".equals(level))
                logger.warn("HttpFileServer", msg);
            else
                logger.info("HttpFileServer", msg);
        } else {
            System.out.println("[" + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "] "
                    + "[HttpFileServer] " + level + " - " + msg);
        }
    }
}
