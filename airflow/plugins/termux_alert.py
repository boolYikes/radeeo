import os
import paramiko

TERMUX_HOST = os.getenv("TERMUX_HOST")
TERMUX_PORT = int(os.getenv("TERMUX_PORT"))
TERMUX_USER = os.getenv("TERMUX_USER")
SSH_KEY      = "/opt/airflow/.ssh/id_ed25519_pwless"

PATTERN = "250,150,500,150,1000"    # vibe-pause-vibe-pause-vibe

def vibrate_alert(context):
    """
    Called by Airflow when any task that has this callback fails.
    Uses SSH to call termux-notification with a vibration pattern.
    """
    title = f"{context['task_instance'].task_id} failed"
    msg   = f"DAG {context['dag'].dag_id} – run {context['run_id']}"
    cmd   = (
        f'termux-notification -t "{title}" -c "{msg}" --vibrate {PATTERN} --action "termux-toast Doom-has-come"'
    )

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(TERMUX_HOST, 
                port=TERMUX_PORT,
                username=TERMUX_USER, 
                key_filename=SSH_KEY)
    ssh.exec_command(cmd)
    ssh.close()
