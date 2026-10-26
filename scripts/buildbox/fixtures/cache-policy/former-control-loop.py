CONTROL.unlink(missing_ok=True)
with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as listener:
    listener.bind(str(CONTROL)); CONTROL.chmod(0o666); listener.listen(64)
    listener.setblocking(False)
    events = selectors.DefaultSelector(); events.register(listener, selectors.EVENT_READ)
    pidfd = os.pidfd_open(owner['pid']); events.register(pidfd, selectors.EVENT_READ)
    notify = os.environ.get('NOTIFY_SOCKET')
    if notify:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as ready:
            ready.connect('\0'+notify[1:] if notify.startswith('@') else notify)
            ready.send(b'READY=1')
    try:
        while True:
            for key, _ in events.select():
                if key.fileobj == pidfd: raise CachePolicyError(detail='host server exited')
                if key.fileobj == listener:
                    connection = listener.accept()[0]; connection.setblocking(False)
                    events.register(connection, selectors.EVENT_READ)
                    continue
                connection = key.fileobj; events.unregister(connection)
                with connection:
                    try:
                        if connection.recv(32) != b'verify': raise CachePolicyError(detail='invalid authority request')
                        reply = {'owner': validate_server(expected=owner)}
                    except (OSError, RuntimeError) as error: reply = {'error': str(error)}
                    try: connection.send(json.dumps(reply).encode())
                    except BrokenPipeError: pass
    finally: os.close(pidfd); events.close(); CONTROL.unlink(missing_ok=True)

