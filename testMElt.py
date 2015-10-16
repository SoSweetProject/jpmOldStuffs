from subprocess import Popen, PIPE
p = Popen(['MElt', 'TKP'],stdin=PIPE,stdout=PIPE, stderr=PIPE)
(stdout, stderr) = p.communicate("premier texte")
print stdout
(stdout, stderr) = p.communicate("second texte")
print stdout
