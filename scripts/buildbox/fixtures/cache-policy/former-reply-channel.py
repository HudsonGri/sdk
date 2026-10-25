def admitted(env):
    canonical = cache_environment(env)
    try:
        protected(CACHE_ROOT)
        # One atomic request carries a private reply socket. The authority
        # never accepts or retains an idle connection or client descriptor.
        with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as request:
            receiver,reply_socket=socket.socketpair(socket.AF_UNIX,socket.SOCK_DGRAM)
            with receiver,reply_socket:
                receiver.setsockopt(socket.SOL_SOCKET,socket.SO_PASSCRED,1)
                request.connect(str(CONTROL))
                request.sendmsg([b'verify'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[reply_socket.fileno()]))])
                data,ancillary,flags,_=receiver.recvmsg(16384,socket.CMSG_SPACE(12))
                credentials=[struct.unpack('3i',value) for level,kind,value in ancillary
                             if level==socket.SOL_SOCKET and kind==socket.SCM_CREDENTIALS]
                if flags & (socket.MSG_TRUNC|socket.MSG_CTRUNC) or len(credentials)!=1 or credentials[0][1]!=0:
                    raise CachePolicyError(detail='authority reply has no root kernel credentials')
                reply=json.loads(data)
        if 'error' in reply: raise CachePolicyError(detail=reply['error'])
        owner = reply['owner']
        if (owner['scope'] != 'host-shared' or owner['endpoint'] != str(CACHE_ENDPOINT)
                or owner['cache_bytes'] != SCCACHE_CACHE_BYTES or not owner['identity']):
            raise CachePolicyError(detail='authority response schema/policy mismatch')
        env.update(canonical)
        return owner
    except CachePolicyError: raise
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise CachePolicyError(detail='authority unavailable: '+str(error)) from error


def verify_request(listener,owner,refusal=None):
    """Consume one bounded packet; every received descriptor closes here."""
    descriptors=[]
    try:
        data,ancillary,flags,_=listener.recvmsg(32,socket.CMSG_SPACE(12)+socket.CMSG_SPACE(4),socket.MSG_CMSG_CLOEXEC)
        credentials=[]
        for level,kind,value in ancillary:
            if level!=socket.SOL_SOCKET:continue
            if kind==socket.SCM_RIGHTS:
                received=array.array('i');received.frombytes(value[:len(value)//received.itemsize*received.itemsize])
                descriptors.extend(received)
            elif kind==socket.SCM_CREDENTIALS:credentials.append(struct.unpack('3i',value))
        if len(descriptors)!=1:return
        descriptor=descriptors.pop()
        try:response=socket.socket(fileno=descriptor)
        except OSError:
            os.close(descriptor);return
        with response:
            try:
                if (data!=b'verify' or flags & (socket.MSG_TRUNC|socket.MSG_CTRUNC)
                        or len(credentials)!=1 or credentials[0][1] not in (0,pwd.getpwnam('var').pw_uid)
                        or response.getsockopt(socket.SOL_SOCKET,socket.SO_TYPE)!=socket.SOCK_DGRAM):
                    raise CachePolicyError(detail='invalid atomic authority request')
                if refusal:raise CachePolicyError(detail=refusal)
                reply={'owner':validate_server(expected=owner)}
            except (OSError,RuntimeError) as error:reply={'error':str(error)}
            try:response.send(json.dumps(reply).encode(),socket.MSG_DONTWAIT|socket.MSG_NOSIGNAL)
            except OSError:pass  # The peer owns a bounded, possibly abandoned reply queue.
    finally:
        for descriptor in descriptors:os.close(descriptor)
