#!/usr/bin/env python
# -*- coding: utf-8 -*-
#script pour tagé les tweets avec Melt, ici on définit la fonction  tagListOfTexts qu'on va utiliser dans meltWrapper.py 
from __future__ import unicode_literals
from subprocess import Popen, PIPE
import re

class meltWrapper():
#self pour céer un instance de la classe, on définit les éléments de Melt
	def __init__(self, MElt_bin, MElt_options):
		self.MElt_bin=MElt_bin
		self.MElt_options=MElt_options
#définition d'une fonction qui prendra comme paramètre la liste des textes à tagés
	def tagListOfTexts(self,liste):	
		#http://unicode-table.com/fr/#000A
		#transformer la liste de chaines en 1 seule chaine et on les découpe à partir de retour à la ligne
		chaine="\n".join(liste)
#pour récuppère le résultat de notre chaine tagée
		res=self.tag(chaine)
		return res
#présentation d'une fonction qui prendra un texte à la rentrée et elle donnera ce texte à Melt 
	def tag(self,texte):
#cmd est la ligne de commande sur linux pour melt(MElt -T -N -K) 
		cmd = [self.MElt_bin, self.MElt_options]
#Popen fait le lien entre les programmes,python et melt
		p = Popen(cmd,stdin=PIPE,stdout=PIPE, stderr=PIPE)
#texte qui est envoyé à cpmmunicate est en unicode et on le transfère en string avec encode mais comme dans strdout=....    on n'affecte pas la valeuer dans texte du coup type reste unicode comme ce qu'on a eu au début  
		(stdout, stderr) = p.communicate(texte.encode('utf-8'))
#communicate nous envoit stdout qui est en string et qu'on le transfère en unicode
		stdout=stdout.decode('utf8')
		print(stdout)			
#un exemple du sortie de stdout:
#Au/P+D/0.90305676909 tel/ADJ/0.976219163052 avec/P/0.99319435586 Stacy/NPP/0.997467420283
#on crée une liste vide pour mettre les résultats de Melt dedans
		texts_tagged=[]
#pour chaque sortie de Melt on le découpe à partir de retour à la ligne, chaque ligne contiendra un seul tweet annoté 
		for line in stdout.split("\n"):		
#tag est la liste des étiquettes de Melt 	  
			tags = 'ADJ|ADJWH|ADV|ADVWH|CC|CLO|CLR|CLS|CS|DET|DETWH|ET|I|NC|NPP|P|P\+D|P\+PRO|PONCT|PREF|PRO|PROREL|PROWH|V|VIMP|VINF|VPP|VPR|VS|KK'
#expression régulière pour récuppèrer les éléments tagés comme:(Au/P+D/0.90305676909)
			exp = u'(?:\{(?P<normalisation>.*?)\} )?(?P<token>\S+)?/(?P<tag>%s)/?(?P<proba>\d\.\d+)?(?: |$|\n)'%tags
#si l'élément du sortie de Melt n'est pas vide:
			if line!="":
#on cherche l'expression régulière dans cette ligne notre expression régulière
				l = re.compile(exp).findall(line)
#on cérée une liste vide
				l2=[]
#pour chaque élément qu'on trouve avec notre expression régulière dans line
				for e in l:
#si le champ consacré à la normalisation n'est pas vide
					if e[0]=='':
#on met un dictionnaire dans la liste qu'on a créé 
						l2.append({'normalization':e[0],'token':e[1],'tag':e[2],'probability':e[3]})
			
					else:
						l2.append({'normalization':e[1],'token':e[0],'tag':e[2],'probability':e[3]})
#on met ces listes de dictionnaire dans un grand dictionnaire qui contiendra tous : comme:[[{'token': 'Au', 'tag': 'P+D', 'probability': '0.90305676909', 'normalization': ''}, {'token': 'tel', 'tag': 'ADJ', 'probability': '0.976219163052', 'normalization': ''}, {'token': 'avec', 'tag': 'P', 'probability': '0.99319435586', 'normalization': ''}, {'token': 'Stacy', 'tag': 'NPP', 'probability': '0.997467420283', 'normalization': ''}], [{'token': '@Alananas_', 'tag': 'NPP', 'probability': '', 'normalization': '_URL'}, {'token': 'a', 'tag': 'V', 'probability': '0.99946496138', 'normalization': ''}, {'token': 'mort', 'tag': 'VPP', 'probability': '0.605760338727', 'normalization': ''}, {'token': '...', 'tag': 'PONCT', 'probability': '', 'normalization': ''}]]	
				texts_tagged.append(l2)
		return texts_tagged

		
		
		
		
		
	
	
	
