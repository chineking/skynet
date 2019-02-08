import time
from mars.actors import Actor, create_actor_pool, Distributor
from mars.actors.pool.utils import new_actor_id
from mars.compat import six


class SkynetDistiributor(Distributor):
    def distribute(self, uid):
        if isinstance(uid, six.text_type) and ':' in uid:
            return int(uid.split(':', 1)[0]) % self.n_process
        elif isinstance(uid, six.binary_type) and b':' in uid:
            return int(uid.split(b':', 1)[0]) % self.n_process

        return super(SkynetDistiributor, self).distribute(uid)


class Skynet(Actor):
    __slots__ = '_parent', '_todo', '_acc', '_start_time', '__weakref__'

    def __init__(self, parent=None):
        super(Skynet, self).__init__()
        self._parent = parent
        self._todo = 10
        self._acc = 0
        self._start_time = None
        if parent is None:
            # root, record start time
            self._start_time = time.time()

    def post_create(self):
        if self._parent is not None:
            self._parent = self.ctx.actor_ref(self._parent)

    def on_receive(self, message):
        if len(message) == 2:
            level, num = message
            if level == 1:
                self._parent.tell((num,))
                self.ref().destroy()
            else:
                start = num * 10
                for i in range(10):
                    if self._parent is None:
                        # root
                        uid = b'%d:%s' % (i, new_actor_id())
                    else:
                        uid = b'%s:%s' % (self.uid.split(b':', 1)[0], new_actor_id())
                    child = self.ctx.create_actor(Skynet, self.ref(), uid=uid)
                    child.tell((level - 1, start + i))
        else:
            self._todo -= 1
            self._acc += message[0]
            if self._todo == 0:
                if self._parent is not None:
                    self._parent.tell((self._acc,))
                else:
                    # root
                    assert self._acc == 499999500000
                    print('cost', time.time() - self._start_time, 'seconds')
                self.ref().destroy()


if __name__ == '__main__':
    n_process = 1
    distributor = SkynetDistiributor(n_process)
    with create_actor_pool(n_process=n_process, distributor=distributor) as pool:
        root = pool.create_actor(Skynet)
        root.tell((7, 0))
        while pool.has_actor(root):
            pool.join(0.1)
