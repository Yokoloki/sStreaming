from cmd import Cmd


class RPC(Cmd):

    def __init__(self, mininet):
        Cmd.__init__(self)
        self.mn = mininet

    def do_link(self, line):
        """Bring link(s) between two nodes up or down.
           Usage: link node1 node2 [up/down]"""
        args = line.split()
        if len(args) != 3:
            # error('invalid number of args: link end1 end2 [up down]\n')
            return
        elif args[2] not in ['up', 'down']:
            # error('invalid type: link end1 end2 [up down]\n')
            return
        else:
            self.mn.configLinkStatus(*args)

    def default(self, line):
        """Called on an input line when the command prefix is not recognized.
           Overridden to run shell commands when a node is the first CLI argument.
           Past the first CLI argument, node names are automatically replaced with
           corresponding IP addrs."""

        first, args, line = self.parseline(line)

        if first in self.mn:
            if not args:
                return
            node = self.mn[first]
            rest = args.split(' ')
            # Substitute IP addresses for node names in command
            # If updateIP() returns None, then use node name
            rest = [self.mn[arg].defaultIntf().updateIP() or arg
                    if arg in self.mn else arg
                    for arg in rest]
            rest = ' '.join(rest)
            # Run cmd on node:
            node.cmd(rest)
            return node.lastPid
        else:
            # error('*** Unknown command: %s\n' % line)
            return None
