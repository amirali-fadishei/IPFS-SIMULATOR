const API_URL = "";

const els = {
    dropZone: document.getElementById("dropZone"),
    fileInput: document.getElementById("fileInput"),
    uploadResult: document.getElementById("uploadResult"),
    cidDisplay: document.getElementById("cidDisplay"),
    copyBtn: document.getElementById("copyBtn"),
    cidInput: document.getElementById("cidInput"),
    dlBtn: document.getElementById("dlBtn"),
    dlLoader: document.getElementById("dlLoader"),
};

const DEFAULT_DROP_TEXT = "Click or Drag file here to upload";

function setDropText(text, isError = false) {
    if (!els.dropZone) return;
    els.dropZone.textContent = "";
    const p = document.createElement("p");
    p.textContent = text;
    if (isError) p.style.color = "red";
    els.dropZone.appendChild(p);
}

function setUploadingState(isUploading) {
    if (!els.dropZone) return;
    els.dropZone.style.pointerEvents = isUploading ? "none" : "auto";
}

function setDownloadingState(isDownloading) {
    if (els.dlBtn) els.dlBtn.disabled = isDownloading || !getCidInput();
    if (els.dlLoader) els.dlLoader.style.display = isDownloading ? "inline-block" : "none";
}

function getCidInput() {
    return (els.cidInput?.value || "").trim();
}

async function handleUpload(file) {
    if (!file) return;

    setUploadingState(true);
    setDropText(`Uploading ${file.name}...`);

    let response;
    try {
        response = await fetch(`${API_URL}/upload`, {
            method: "POST",
            headers: { "X-Filename": file.name },
            body: file,
        });
    } catch (err) {
        console.error(err);
        setDropText("Network Error: Server is unreachable", true);
        setUploadingState(false);
        return;
    }

    if (!response.ok) {
        setDropText(`Upload Failed: ${response.status} ${response.statusText}`, true);
        setUploadingState(false);
        return;
    }

    try {
        const data = await response.json();
        const cid = (data?.cid || "").toString();

        if (els.cidDisplay) els.cidDisplay.textContent = cid;
        if (els.uploadResult) els.uploadResult.classList.add("show");

        if (els.cidInput) els.cidInput.value = cid;

        setDownloadingState(false);

        setDropText("Done!");
        setTimeout(() => setDropText(DEFAULT_DROP_TEXT), 1500);
    } catch (err) {
        console.error("JSON Parse Error:", err);
        setDropText("Invalid Server Response", true);
    } finally {
        setUploadingState(false);
    }
}

async function copyCid() {
    const text = (els.cidDisplay?.textContent || "").trim();
    if (!text) return;

    try {
        await navigator.clipboard.writeText(text);
        if (els.copyBtn) {
            const old = els.copyBtn.textContent;
            els.copyBtn.textContent = "Copied!";
            setTimeout(() => (els.copyBtn.textContent = old || "Copy CID"), 1000);
        }
    } catch (err) {
        console.error("Failed to copy:", err);
        alert("Failed to copy CID");
    }
}

async function downloadFile() {
    const cid = getCidInput();
    if (!cid) return alert("Please enter a CID");

    setDownloadingState(true);

    let response;
    try {
        response = await fetch(`${API_URL}/download?cid=${encodeURIComponent(cid)}`);
    } catch (err) {
        console.error(err);
        alert("Network Error: Could not connect to gateway");
        setDownloadingState(false);
        return;
    }

    if (!response.ok) {
        alert("Download failed: CID not found or Engine error");
        setDownloadingState(false);
        return;
    }

    try {
        const blob = await response.blob();
        const url = URL.createObjectURL(blob);

        const a = document.createElement("a");
        a.href = url;
        a.download = `${cid}.bin`;
        document.body.appendChild(a);
        a.click();
        a.remove();

        URL.revokeObjectURL(url);
    } catch (err) {
        console.error("Blob Error:", err);
        alert("Error processing file download");
    } finally {
        setDownloadingState(false);
    }
}

els.dropZone?.addEventListener("click", () => els.fileInput?.click());

els.dropZone?.addEventListener("dragover", (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
    els.dropZone.classList.add("dragover");
});

els.dropZone?.addEventListener("dragleave", () => {
    els.dropZone.classList.remove("dragover");
});

els.dropZone?.addEventListener("drop", (e) => {
    e.preventDefault();
    els.dropZone.classList.remove("dragover");
    const file = e.dataTransfer.files?.[0];
    handleUpload(file).catch(console.error);
});

els.fileInput?.addEventListener("change", () => {
    const file = els.fileInput.files?.[0];
    handleUpload(file).catch(console.error);
    els.fileInput.value = "";
});

els.copyBtn?.addEventListener("click", copyCid);

els.dlBtn?.addEventListener("click", () => {
    downloadFile().catch(console.error);
});

els.cidInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") downloadFile().catch(console.error);
});

els.cidInput?.addEventListener("input", () => {
    setDownloadingState(false);
});

setDropText(DEFAULT_DROP_TEXT);
setDownloadingState(false);