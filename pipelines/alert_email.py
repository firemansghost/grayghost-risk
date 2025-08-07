import os, smtplib, ssl, json
from email.message import EmailMessage
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

def send_mail(subject, body, to_emails):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("SMTP_FROM", "alerts@example.com")
    msg["To"] = ", ".join(to_emails)
    msg.set_content(body)

    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ["SMTP_USER"]
    pwd  = os.environ["SMTP_PASS"]

    ctx = ssl.create_default_context()
    with smtplib.SMTP(host, port) as server:
        server.starttls(context=ctx)
        server.login(user, pwd)
        server.send_message(msg)

if __name__ == "__main__":
    # Compare previous band to current; if flip across thresholds, notify
    latest_text = (DATA / "latest.json").read_text()
doc = json.loads(latest_text)
current_band = doc.get("band", "yellow")
    prev_band_file = DATA / "prev_band.txt"
    prev_band = prev_band_file.read_text().strip() if prev_band_file.exists() else "yellow"

    if current_band != prev_band:
        recipients = [e.strip() for e in os.environ.get("ALERT_EMAILS","").split(",") if e.strip()]
        if recipients:
            send_mail(
                subject=f"[GrayGhost Risk] Band flip: {prev_band.upper()} → {current_band.upper()}",
                body=f"Risk band changed from {prev_band} to {current_band}. See dashboard for details.",
                to_emails=recipients
            )
    prev_band_file.write_text(current_band)
