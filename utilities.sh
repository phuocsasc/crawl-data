#!/bin/bash
sed -i 's/#\(PermitRootLogin\).*/\1 yes/' /etc/ssh/sshd_config