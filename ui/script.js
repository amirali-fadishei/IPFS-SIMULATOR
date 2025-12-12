// web_ui/script.js

const fileInput = document.getElementById('fileInput');
const resultCid = document.getElementById('result-cid');
const statusMessage = document.getElementById('status-message');

/**
 * مدیریت کلیک بر روی دکمه آپلود
 */
function handleUpload() {
    const file = fileInput.files[0];

    if (!file) {
        statusMessage.textContent = 'لطفا یک فایل را انتخاب کنید.';
        statusMessage.style.color = 'red';
        return;
    }

    resultCid.textContent = 'در حال آپلود و پردازش...';
    statusMessage.textContent = 'فایل در حال ارسال به Gateway و Engine است.';
    statusMessage.style.color = 'blue';

    // استفاده از FormData برای ارسال فایل و نام آن
    const formData = new FormData();
    formData.append('file', file);
    // نام فایل را هم ارسال می‌کنیم، هرچند Gateway نیازی به آن ندارد
    // در این پروژه، داده‌های باینری فایل مهم است.
    
    // ارسال درخواست به اندپوینت /upload در main.py
    fetch('/upload', {
        method: 'POST',
        body: formData // Fetch به طور خودکار Content-Type مناسب را تنظیم می‌کند
    })
    .then(response => {
        if (!response.ok) {
            // اگر Gateway خطای HTTP داد (مثلا 502)
            return response.json().then(err => { throw new Error(err.message || 'خطای سرور'); });
        }
        return response.json(); // انتظار پاسخ JSON حاوی CID
    })
    .then(data => {
        if (data && data.cid) {
            resultCid.textContent = data.cid;
            statusMessage.textContent = 'آپلود با موفقیت انجام شد.';
            statusMessage.style.color = 'green';
        } else {
            throw new Error('پاسخ سرور نامعتبر است (CID یافت نشد).');
        }
    })
    .catch(error => {
        console.error('Upload Error:', error);
        resultCid.textContent = 'خطا';
        statusMessage.textContent = `خطا در آپلود: ${error.message}`;
        statusMessage.style.color = 'red';
    });
}