import memcache
import sys

key="usertable-user4288574356201733490"
value="""{"field1":")Sw'66)8|>2z.H;!<>29(1Bq&R5,_;-+$-Kk5Z2X*Ve.Qe>#h>F%,Va/P5;400[11D?<!0%B#5R)?^9 K3<Fm.M/-C%8?>!Qg>","field0":"%78%(x/*z:'.,32<A#-6z:8<75r:V7-_-9Q96/b2I?B/0/\":X/!':4)88W7\"\"d?2j,My.1b4Ra\"V+(1j'%n+0<( (+_y3'x!@{?","field7":":Dm'\"h\"$r/E!8T3$[/&)n>T7,]% Z{&,.-<\":L9$;t&,f#5~3D3>41X3=_7'Mk3*l41 (*,><p3=>5/450r'7,8R{)K;<?f8K}>","field6":")>&,Hk7.j;9|7\\'\\a)E!%$d=@%=Ua0S=-38>-z/_;\";h;\\)4G3990', 5Ky=Gg;^79+l7?&33<9R{6# <4>+;( Yw1Ua.N55. 5","field9":"<7$-#6'1l+Xw?I'906!=~75$!;6<I9-%r##h)$81L?Ha6V5=F=15$++|2.z0A)8@-'H+1Km:A;<@;3\\=$]m0Za\"#:<[1)M8U'5","field8":"2Qi?1*& r>;p8I%5Qm09h2F#!Dy2.v29r38<'0*(B/;+((Cy;;p&P=C7=#`9Sq2V!4@/=Xq/%4(24=4z;6x2&f+(l;_{3Fm72p\"","field3":"7>0,7x%*f;N<Vc:<z(%4)%h%V%-0<!K=0Yu0[m2Zo+#t'L#9:n:P#+V/1Yi#!|\",.,6x?$|!!b+7b-)4)@7_q.2f1S3)&`,^-=","field2":".;h6\"2'-(,V+.Nu3[q,7h:[##!~;!0/Hc+7>7Ug# l=/4=Mk$*h*Fm*6t:2$6Y?0Os%=*1M!\"9r75l=P%8'&><044h=9p#E=84`7","field5":"3(l7D=?.p+\\}/<\"-_a!4:)+l(W+9@/<Ps+>8!/|+O7)Cu3<2*%\")Y):C981|2I)-0<&$l'2j= \"!9.;A',<|#Nc&>r('\"9+,?F}2","field4":"* n2G18&.=@{/?~?M%,^;$947\\9)<x?E/ Jw>/:9:f#%h?'j&S#5Js1(6-?j-3r/1$)/f/\":$?((Fy7Yu&?d$>61(6-X%9?$;% 0"}"""

address = sys.argv[1]
port = int(sys.argv[2])
mc = memcache.Client([address + ':' + str(port)], debug=0)
mc.set(key,value)

while(True):
	print ".",
	mc.get(key)
