#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module consists of the reverse-engineered memcached ascii protocol.

The main assumption we are making is that all the data required for the
query comes within a single TCP packet, not sure if this scales to
larger queries.


Storage Commands
----------------
<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n

eg:
add usertable-user2071219101098386137 0 2147483647 1139
{"field1":"#&*)7z,[}\"A?<#~75z:#>=W19Q98\\q$Mm.>0 Wg75*$A-9R' M31X-%+6%O30@'40r0L50+0+Em'06*2:$M#)A#9Oe-<:8)|:K7<","field0":"'V#0+&-Kw/?v3D7!J'3R!),p6>|&!b3(r>5d$(4%.p1$0()r56b/W}8Ja\"]+4M?4O1;6p/Wc.-p'Hk9Fa'1z+5$)%h94j;_)8)&,","field7":"$Y5-\\+)1t:>|$%x>Xi<No;?t+T?%G/*?d/?6<I%9\"<;S!8/2<Ys*\\%*%.4]?>Y!<Lw/L7Mu.<*8%8*/4%)j64b\"-4/$6-64=,l#","field6":"4/ )S!2/ !Ou8, </d8Pk6\"z= 6,Rw7]o.V#)Ze34$8\":81\"*J+)Cq81`&B).W!)705=f6F+4Eq:[}%Y?<*$?Fi; v50, -b/G-,","field9":"!2,8N{*Yy.Cc%>d3/f'Iw7\\?7:8%2v/S),$t+$(1V-5:$-p.[%0Gw+(b.82;O)-).15l786;)x' l9C/^/)Xy3;.1B94 <6F19","field8":"4'x6-2?&b61`2Vu/@{05h#[-?:205(1'~'Y',!~#1f$N7%Ke)+ %8.-\"(=,4+^g6\\#-(l660,?d'Y%7!>5F{4,r; 27!z7M#6<.!","field3":"4?t2)d%I?$Fc9\\{.;d*>v>>|&?<=B}!K9,5 ![; Ay(^3( 49N-0 &+Aw*Rm0P9%@!*P90C%;T=9.`0S;$Au)Q}?:<1O+=#p1&>!","field2":"+C',_-$968Si!=~>Bc(Hy&Q}#1v3#4)-.9@y(Qq*3$*Xm;S78A%9.4:W/4'.%?0-Z+2Ck'W+?F/(!h0=p:^u')z7' \"$b6F++I)9","field5":")M9%_!4Hk#,*4)p6U=-T)(8: Js;+0+&.584.,:$  >$85Nu?-d#]s-Fy2!p:I{;%f;Xc#[;1(|69&8$j#Ng5*<5/&/Ao\"*.;U3$","field4":"4$x/Lo'T{6Jy87t*>d-Pk&*83Qc7Nc3\"64:t$Ru+C74<&=Lw2R}6)b/M94 >:\\o/*t2..=Ro%/0%P/7V3 /~!4r+W-0I9)B4Fa*"}

"""


class MemcachedQuery(object):
    def __init__(self, fullasciiquery):
        self.command = None
        self.key = None
        self.flags = 0
        self.exptime = 0
        self.bytes = 0
        try:  # Store Command
            first_line, second_line = fullasciiquery.splitlines()
            self.command, self.key, flag_string, exptime_string, bytes_string = first_line.split()
            self.flag = int(flag_string)
            self.exptime = int(exptime_string)
            self.bytes = int(bytes_string)
            self.data = second_line.strip()
        except:  # Get / Delete Command
            try:
                self.command, self.key = fullasciiquery.split()
            except:  # Meta Command:
                self.command = fullasciiquery.strip()
        # TODO: Query subtype parsing if required
