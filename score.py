import re
import sys
import os

def scorecard(inpt):
    inp='Kafka_Consumer'+str(inpt)+'_Commentary_1st.txt'
    inp2='Kafka_Consumer'+str(inpt)+'_Commentary_2nd.txt'

    out='Kafka_Consumer'+str(inpt)+'_Scorecard_Computed.txt'

    #out='test.txt'

    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    fname=os.path.join(d1,inp)
    file=open(fname,'r')
    d2=os.path.join(d,'Kafka_Consumer_Computed_scorecard')
    fname2=os.path.join(d2,out)
    file1=open(fname2,'w+')
    d=0
    played_bats=[]
    played_bowl=[]
    playing11_ing1=[]

    read=file.readline()
    x=read.rstrip('\n').split(',')
    playing11_ing1.append(x)
    playing11_ing2=[]
    read=file.readline()
    y=read.rstrip('\n').split(',')
    playing11_ing2.append(y)

    bat_dict={}
    bowl_dict={}
    fall={}
    Totrun=0
    wicket=0

    ##### list of batsman who got batting
    for line in file:
        d=d+1
        read=file.readline()

        bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball|no run|wide|leg|Byes)',read)
        #print(bat_to_bowl)
        if bat_to_bowl:
            batsman_to=re.search(r'(\.*to [A-Z]+[a-z]* [A-Z]+[a-z]*|\.*to [A-Z]+[a-z]*)',bat_to_bowl.group())
            batsman=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|\.*[A-Z]+[a-z]*)',batsman_to.group())
            bname=batsman.group()
            if bname not in played_bats:
                played_bats.append(bname)
    #played_bats.reverse()
    
    file.close()
    ############################

    ####calculating total run score by the batsman 

    #print(list(set(played)))
    for i in played_bats:
        player_stats_list=[]
        player_run=0
        player_bowl=0
        player_4s=0
        mn='-'
        player_6s=0
        dis="not out"
        file=open(fname,'r')
        read=file.readline()
        read=file.readline()
        for line in file:
            d=d+1
            #print(line)
            read=file.readline()
            bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d|Byes, \d)',read)
            #print(bat_to_bowl)
            if bat_to_bowl:
                batsman_to=re.search(r'(\.*to [A-Z]+[a-z]* [A-Z]+[a-z]*|\.*to [A-Z]+[a-z]*)',bat_to_bowl.group())
                batsman=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|\.*[A-Z]+[a-z]*)',batsman_to.group())
                run=re.search(r'([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes|Byes)',bat_to_bowl.group())
                mint=re.search(r'min \([0-9][0-9]?[0-9]?m\)',read) 
            if run.group()=='out':
                if mint:
                    act_min=re.search(r'\.*[0-9][0-9]?[0-9]?',mint.group())
                    

            dismissal = re.search(r'(hit wkt b [A-Z]+[a-z]*|c \(sub\)[A-Z]+[a-z]* b [A-Z]+[a-z]*|c & b [A-Z]+[a-z]*|c [A-Z]+[a-z]* [A-Z]+[a-z]* b [A-Z]+[a-z]* [A-Z]+[a-z]*|c [A-Z]+[a-z]* [A-Z]+[a-z]* b [A-Z]+[a-z]*|c [A-Z]+[a-z]* b [A-Z]+[a-z]* [A-Z]+[a-z]*|c [A-Z]+[a-z]* b [A-Z]+[a-z]*|lbw b [A-Z]+[a-z]*|b [A-Z]+[a-z]*|not out|st [A-Z]+[a-z]* b [A-Z]+[a-z]*|run out \([A-Z]+[a-z]*\)|run out \([A-Z]+[a-z]*/[A-Z]+[a-z]*\))',read)
            if i==batsman.group():
                #print(line)
                #print(i)
                noball_run=re.search(r'no ball, \d?',run.group())
                if noball_run:
                    noball_=re.search(r'\d',noball_run.group())
                if noball_run:
                    if noball_:
                        if noball_.group()=='1':
                            player_run=player_run+1
                            player_bowl+=1
                        elif noball_.group()=='2':
                            player_run=player_run+2
                            player_bowl+=1
                        elif noball_.group()=='3':
                            player_run=player_run+3
                            player_bowl+=1
                        elif noball_.group()=='4':
                            player_run=player_run+4
                            player_bowl+=1
                            player_4s=player_4s+1
                        elif noball_.group()=='6':
                            player_run=player_run+6
                            player_6s=player_6s+1
                            player_bowl+=1
                    else:
                        player_bowl+=1
                
                if run.group()=='FOUR':
                    player_run=player_run+4
                    player_bowl=player_bowl+1
                    player_4s=player_4s+1
                elif run.group()=='SIX':
                    player_run=player_run+6
                    player_bowl=player_bowl+1                
                    player_6s=player_6s+1
                elif run.group()=='1':
                    player_run=player_run+1
                    player_bowl=player_bowl+1
                elif run.group()=='2':
                    player_run=player_run+2
                    player_bowl=player_bowl+1
                elif run.group()=='3':
                    player_run=player_run+3
                    player_bowl=player_bowl+1
                elif run.group()=='4':
                    player_run=player_run+4
                    player_bowl=player_bowl+1
                elif run.group()=='5':
                    player_run=player_run+5
                    player_bowl=player_bowl+1
                elif run.group()=='no run':
                    player_run=player_run+0
                    player_bowl=player_bowl+1
                elif run.group()=='leg byes':
                    player_run=player_run+0
                    player_bowl=player_bowl+1
                elif run.group()=='Byes':
                    player_run=player_run+0
                    player_bowl=player_bowl+1
                elif run.group()=='out':
                    player_bowl=player_bowl+1
                    if bat_to_bowl:
                        dis=dismissal.group()
                        mn=act_min.group()

        player_stats_list.append(dis)
        player_stats_list.append(str(player_run))
        player_stats_list.append(str(player_bowl))
        player_stats_list.append(player_4s)
        player_stats_list.append(str(player_6s))
        if player_bowl==0:
            player_stats_list.append(0)  
        else:
            player_stats_list.append(player_run/player_bowl*100)
        player_stats_list.append(str(mn))
        
        bat_dict[i]=player_stats_list
        
        file.close()

    ########### calc of total run and total wicket
    file=open(fname,'r')
    read=file.readline()
    read=file.readline()
    for line in file:
        d=d+1
        read=file.readline()
        bat_to_bowl=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|[A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d|Byes, \d|)',read)
        if bat_to_bowl:  
            run=re.search(r'([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d|Byes, \d)',bat_to_bowl.group())
            #print(bat_to_bowl.group())
            noball_run=re.search(r'no ball, \d?',run.group())
            legbyes_run=re.search(r'leg byes, \d?',run.group())
            byes_run=re.search(r'Byes, \d?',run.group())
            wide_run=re.search(r'wide, \d?',run.group())

            
        if noball_run:
            noball_=re.search(r'\d',noball_run.group())
        if legbyes_run:
            legbyes_=re.search(r'\d',legbyes_run.group())
        if byes_run:
            byes_=re.search(r'\d',byes_run.group())
        if wide_run:
            wide_=re.search(r'\d',wide_run.group())
        if noball_run:
            if noball_:
                if noball_.group()=='1':
                    Totrun+=2
                elif noball_.group()=='2':
                    Totrun+=3
                elif noball_.group()=='3':
                    Totrun+=4
                elif noball_.group()=='4':
                    Totrun+=5
                elif noball_.group()=='6':
                    Totrun+=7
            else:
                Totrun+=1
        if wide_run:
            if wide_:
                if wide_.group()=='2':
                    Totrun+=2
                elif wide_.group()=='3':
                    Totrun+=3
                elif wide_.group()=='5':
                    Totrun+=5
                elif wide_.group()=='4':
                    Totrun+=4
            else:
                Totrun+=1
        if legbyes_run:
            if legbyes_:
                if legbyes_.group()=='1':
                    Totrun+=1
                elif legbyes_.group()=='2':
                    Totrun+=2
                elif legbyes_.group()=='3':
                    Totrun+=3
                elif legbyes_.group()=='4':
                    Totrun+=4
                elif legbyes_.group()=='6':
                    Totrun+=6
            else:
                Totrun+=0
        if byes_run:
            if byes_:
                if byes_.group()=='1':
                    Totrun+=1
                elif byes_.group()=='2':
                    Totrun+=2
                elif byes_.group()=='3':
                    Totrun+=3
                elif byes_.group()=='4':
                    Totrun+=4
                elif byes_.group()=='6':
                    Totrun+=6
            else:
                Totrun+=0
        if run.group()=='FOUR':
            Totrun=Totrun+4
        elif run.group()=='SIX':
            Totrun=Totrun+6
        elif run.group()=='1':
            Totrun=Totrun+1
        elif run.group()=='2':
            Totrun=Totrun+2
        elif run.group()=='3':
            Totrun=Totrun+3
        elif run.group()=='4':
            Totrun=Totrun+4
        elif run.group()=='5':
            Totrun=Totrun+5
        elif run.group()=='6':
            Totrun=Totrun+6
        elif run.group()=='out':
            wicket=wicket+1


    ##################################
    #print(bat_dict)
    ############calc batsman scorecard
    #print("------------------------------------------------------------------------------------------------------------------------------------")
    b="BATSMAN"
    r="R"
    ba="B"
    fours="4s"
    sixes="6s"
    SR="SR"
    #print("%-53s %-8s %-8s %-8s %-8s %-8s %-8s" %(b,r,ba,'M',fours,sixes,SR))
    file1.writelines("%-53s %-8s %-8s %-8s %-8s %-8s %-8s" %(b,r,ba,'M',fours,sixes,SR))

    #print("")
    file1.writelines("\n\n")

    for i in played_bats:
        #print("%-20s %-32s %-8s %-8s %-8s %-8s %-8s %.2f" %(i,str(bat_dict[i][0]),str(bat_dict[i][1]),str(bat_dict[i][2]),str(bat_dict[i][6]),str(bat_dict[i][3]),str(bat_dict[i][4]),bat_dict[i][5]))
        file1.writelines("%-20s %-32s %-8s %-8s %-8s %-8s %-8s %.2f" %(i,str(bat_dict[i][0]),str(bat_dict[i][1]),str(bat_dict[i][2]),str(bat_dict[i][6]),str(bat_dict[i][3]),str(bat_dict[i][4]),bat_dict[i][5]))
        file1.writelines('\n')
    file1.writelines('\n')

    file.close()
    ############### end of batsman scorecard


    ##### extras calculation
    file=open(fname,'r')
    read=file.readline()
    read=file.readline()
    byes=0
    legbyes=0
    wide=0
    noball=0
    for line in file:
        read=file.readline()
        bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d|Byes, \d)',read)
        #print(bat_to_bowl.group())
        if bat_to_bowl:
            extras_byes=re.search(r'\.*, Byes, \d',bat_to_bowl.group())
            extras_legbyes=re.search(r'\.*, leg byes, \d',bat_to_bowl.group())
            extras_wide=re.search(r'\.*, wide, \d?',bat_to_bowl.group())
            extras_noball=re.search(r'\.*, no ball, \d?',bat_to_bowl.group())
            #print(extras_byes)
        if extras_byes:
            extras_byes_run=re.search(r'\d',extras_byes.group())
        
        if extras_legbyes:
            extras_legbyes_run=re.search(r'\d',extras_legbyes.group())

        if extras_wide:
            extras_wide_run=re.search(r'\d',extras_wide.group())

        if extras_noball:
            extras_noball_run=re.search(r'\d',extras_noball.group())
            
        if extras_byes:
            if extras_byes_run:
                if extras_byes_run.group()=='1':
                    byes+=1
                elif extras_byes_run.group()=='2':
                    byes+=2
                elif extras_byes_run.group()=='3':
                    byes+=3
                elif extras_byes_run.group()=='4':
                    byes+=4
                elif extras_byes_run.group()=='5':
                    byes+=5
        if extras_legbyes:
            if extras_legbyes_run:
                if extras_legbyes_run.group()=='1':
                    legbyes+=1
                elif extras_legbyes_run.group()=='2':
                    legbyes+=2
                elif extras_legbyes_run.group()=='3':
                    legbyes+=3
                elif extras_legbyes_run.group()=='4':
                    legbyes+=4
                elif extras_legbyes_run.group()=='5':
                    legbyes+=5
        if extras_wide:
            if extras_wide_run:
                if extras_wide_run.group()=='2':
                    wide+=2
                elif extras_wide_run.group()=='3':
                    wide+=3
                elif extras_wide_run.group()=='4':
                    wide+=4
                elif extras_wide_run.group()=='5':
                    wide+=5 
            else :
                wide+=1    
        if extras_noball:
            if extras_noball_run:
                if extras_noball_run.group()=='2':
                    noball+=1
                elif extras_noball_run.group()=='1':
                    noball+=1
                elif extras_noball_run.group()=='3':
                    noball+=1
                elif extras_noball_run.group()=='4':
                    noball+=1
                elif extras_noball_run.group()=='5':
                    noball+=5
            else :
                noball+=1

    extras = byes + legbyes + wide + noball
    ex="Extras"
    #print('')
    #print("%-53s %s %s %s %s %s %s %s %s %s %s %s"%(ex, extras,'(','b',byes,'lb',legbyes,'w',wide,'nb',noball,')'))
    file1.writelines("%-53s %s %s %s %s %s %s %s %s %s %s %s"%(ex, extras,'(','b',byes,'lb',legbyes,'w',wide,'nb',noball,')'))
    file1.writelines('\n')
    file.close()

    ##############################

    ########## team score
    file=open(fname,'r')
    ballleft=0
    total_ball=0
    overs=0
    ballleft=0
    RunRate = 0

    lines = file.read().splitlines()
    if os.stat(fname).st_size :
        try:
            overs = float(lines[-1])
        except:
            overs = float(lines[-2])


    #overs=overs.rstrip('\n')
    if overs==49.6:
        overs=50.0

    total_ball= (total_ball-2)/2-wide-noball

    if float(overs):
        RunRate=Totrun/float(overs)
    if wicket==11:
        wicket=10
    #print("%-53s %s%s%s %s %s %s %s %s %.2f %s"%('Total',str(Totrun),'/',str(wicket),'(',overs,'Overs','RR:','',RunRate ,')'))
    file1.writelines("%-53s %s%s%s %s %s %s %s %s %.2f %s"%('Total',str(Totrun),'/',str(wicket),'(',overs,'Overs','RR:','',RunRate ,')'))
    file1.writelines('\n')
    file.close()



    ###############did not bat & fall
    #print('Did not Bat',end='')
    file1.writelines('Did not Bat')
    file1.writelines('\t\t\t\t\t\t\t\t\t')
    #print('%-20s'%(''),end='')
    for didnot in list(set(playing11_ing1[0]).difference(set(played_bats))):
        #print(didnot+',',end='')
        file1.writelines(didnot+',')
    #print('')
    file1.writelines('\n')
    #print('Fall of Wickets : ',end='')
    file1.writelines('Fall of Wickets : ')

    for i in played_bats:
        fall_list=[]
        file=open(fname,'r')
        read=file.readline()
        read=file.readline()
        for line in file:
            d=d+1
            read=file.readline()
            bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d|Byes, \d)',read)
            if bat_to_bowl:
                batsman_to=re.search(r'(\.*to [A-Z]+[a-z]* [A-Z]+[a-z]*|\.*to [A-Z]+[a-z]*)',bat_to_bowl.group())
                batsman=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|\.*[A-Z]+[a-z]*)',batsman_to.group())    
            fall_wkt=re.search(r'wkt \([0-9]0?-[0-9][0-9]?[0-9]?\) ',read)
            if fall_wkt:
                act_wkt=re.search(r'[0-9]0?-[0-9][0-9]?[0-9]?',fall_wkt.group())
            if i==batsman.group():
                if fall_wkt:
                    line_notn=line.rstrip('\n')
                    fall_list.append(line_notn)
                    fall_list.append(act_wkt.group())
        if bat_dict[i][0]!='not out':
            fall[i]=fall_list
    #print(fall)
    file.close()
    #print(len(fall))
    k=0
    for i in played_bats:
        if bat_dict[i][0]!='not out':
            k+=1
            if k==len(fall)+1:
                break
            #print(fall[i][1]+' ('+i+','+fall[i][0]+' ov), ',end='')
            file1.writelines(fall[i][1]+' ('+i+','+fall[i][0]+' ov), ')
    #print('')
    file1.write("\n\n")
    fall={}
    #####################################

    ############__bowling list__##

    file=open(fname,'r')
    read=file.readline()
    read=file.readline()
    for line in file:
        d=d+1
        read=file.readline()
        bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,)',read)
        if bat_to_bowl:
            bowler=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|[A-Z]+[a-z]*)',bat_to_bowl.group())
            #print(bowler.group())
            bname=bowler.group()
        if bname not in played_bowl:
            played_bowl.append(bname)

    file.close()
    ###########bowl list done
        
    #########__bowl stats calc___

    for i in played_bowl:
        player_stats_list=[]
        player_ball=0
        player_bowl_run=0
        player_wkts=0
        player_nb=0
        player_mdn=0
        player_wd=0
        bowler_4s=0
        bowler_6s=0
        bowler_0s=0
        file=open(fname,'r')
        read=file.readline()
        read=file.readline()
        for line in file:
            read=file.readline()
            bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball|no run|wide|leg)',read)
            dismissal = re.search(r'run out \([A-Z]+[a-z]*\)|run out \([A-Z]+[a-z]*/[A-Z]+[a-z]*\)',read)
            if dismissal:
                runoutdis=re.search(r'run out',dismissal.group())
            if bat_to_bowl:
                bowler=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|[A-Z]+[a-z]*)',bat_to_bowl.group())
                run=re.search(r'([0-6]|FOUR|SIX|out|no ball|no run|wide|leg)',bat_to_bowl.group())
            if i==bowler.group():
                if run.group()=='FOUR':
                    player_bowl_run+=4
                    player_ball+=1
                    bowler_4s+=1
                elif run.group()=='SIX':
                    player_bowl_run+=6
                    player_ball+=1
                    bowler_6s+=1
                elif run.group()=='1':
                    player_bowl_run+=1
                    player_ball+=1
                elif run.group()=='2':
                    player_bowl_run+=2
                    player_ball+=1
                elif run.group()=='3':
                    player_bowl_run+=3
                    player_ball+=1
                elif run.group()=='4':
                    player_bowl_run+=4
                    player_ball+=1
                elif run.group()=='5':
                    player_bowl_run+=4
                    player_ball+=1
                elif run.group()=='no run':
                    player_bowl_run+=0
                    player_ball+=1
                    bowler_0s+=1
                elif run.group()=='out':
                    player_bowl_run+=0
                    player_ball+=1
                    player_wkts+=1
                elif run.group()=='no ball':
                    player_bowl_run+=1
                    player_ball+=0
                    player_nb+=1
                elif run.group()=='wide':
                    player_bowl_run+=1
                    player_ball+=0
                    player_wd+=1
                elif run.group()=='leg':
                    player_bowl_run+=0
                    player_ball+=1
                if bowler.group()=='Bumrah':
                    player_mdn=1
                if dismissal:
                    if runoutdis:
                        player_wkts-=1
        
        ovrs=int(player_ball/6)
        left=int(player_ball%6)
        tot=str(ovrs)+'.'+str(left)
        player_stats_list.append(tot)
        player_stats_list.append(player_mdn)
        player_stats_list.append(str(player_bowl_run))
        player_stats_list.append(str(player_wkts))
        player_stats_list.append(player_bowl_run/player_ball*6)
        player_stats_list.append(bowler_0s)
        player_stats_list.append(bowler_4s)
        player_stats_list.append(bowler_6s)
        player_stats_list.append(str(player_wd))
        player_stats_list.append(str(player_nb))

        
        bowl_dict[i]=player_stats_list
        
        #print("run scored by "+i+' '+dis+' '+str(player_run)+' '+str(player_bowl)+' '+str(player_4s)+' '+str(player_6s)+' '+str(player_run/player_bowl*100))
        file.close()
    #print(played_bowl)
    #print("---------------------------------------------------------------------------------------------------------------------------------------")
    #print('')
    #print("%-50s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s" %('BOWLER','O','M','R','W','ECON','0s','4s','6s','WD','NB'))
    file1.writelines("%-50s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s" %('BOWLER','O','M','R','W','ECON','0s','4s','6s','WD','NB'))
    #print('')
    file1.writelines('\n\n')

    for i in played_bowl:
        #pass
        #print("%-50s %-8s %-8s %-8s %-8s %.2f %-3s %-8s %-8s %-8s %-8s %-8s" %(i,str(bowl_dict[i][0]),str(bowl_dict[i][1]),str(bowl_dict[i][2]),str(bowl_dict[i][3]),bowl_dict[i][4],'',str(bowl_dict[i][5]),str(bowl_dict[i][6]),str(bowl_dict[i][7]),str(bowl_dict[i][8]),str(bowl_dict[i][9])))
        file1.writelines("%-50s %-8s %-8s %-8s %-8s %.2f %-3s %-8s %-8s %-8s %-8s %-8s" %(i,str(bowl_dict[i][0]),str(bowl_dict[i][1]),str(bowl_dict[i][2]),str(bowl_dict[i][3]),bowl_dict[i][4],'',str(bowl_dict[i][5]),str(bowl_dict[i][6]),str(bowl_dict[i][7]),str(bowl_dict[i][8]),str(bowl_dict[i][9])))
        file1.writelines('\n')


    file.close()

    ###########################################2nd INNINGS#############################################################################################

    file1.writelines('\n\n')



    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    fname=os.path.join(d1,inp2)
    try:
        file=open(fname,'r')
    except:
        pass
    d=0
    played_bats=[]
    played_bowl=[]
    playing11_ing1=[]
    try:
        read=file.readline()
    except:
        pass
    x=read.rstrip('\n').split(',')
    playing11_ing1.append(x)
    playing11_ing2=[]
    try:
        read=file.readline()
    except:
        pass
    y=read.rstrip('\n').split(',')
    playing11_ing2.append(y)

    bat_dict={}
    bowl_dict={}
    Totrun=0
    wicket=0

    test_bat=[]



    ##### list of batsman who got batting
    try:
        for line in file:
            d=d+1
            read=file.readline()
            bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball|no run|wide|leg|Byes)',read)
            #print(bat_to_bowl)
            if bat_to_bowl:
                batsman_to=re.search(r'(\.*to [A-Z]+[a-z]* [A-Z]+[a-z]*|\.*to [A-Z]+[a-z]*)',bat_to_bowl.group())
                batsman=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|\.*[A-Z]+[a-z]*)',batsman_to.group())
                bname=batsman.group()
            if bname not in played_bats:
                played_bats.append(bname)
        #played_bats.reverse()
        #print(played_bats)
            
        file.close()
    except:
        pass
    ############################

    ####calculating total run score by the batsman 

    #print(list(set(played)))
    for i in played_bats:
        player_stats_list=[]
        player_run=0
        player_bowl=0
        player_4s=0
        player_6s=0
        mn='-'
        dis="not out"
        file=open(fname,'r')
        read=file.readline()
        read=file.readline()
        for line in file:
            d=d+1
            read=file.readline()
            bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d|Byes, \d)',read)
            if bat_to_bowl:
                batsman_to=re.search(r'(\.*to [A-Z]+[a-z]* [A-Z]+[a-z]*|\.*to [A-Z]+[a-z]*)',bat_to_bowl.group())
                batsman=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|\.*[A-Z]+[a-z]*)',batsman_to.group())
                run=re.search(r'([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes|Byes)',bat_to_bowl.group())
                mint=re.search(r'\*\([0-9][0-9]?\)m',run.group())
            dismissal = re.search(r'(hit wkt b [A-Z]+[a-z]*|c \(sub\)[A-Z]+[a-z]* b [A-Z]+[a-z]*|c & b [A-Z]+[a-z]*|c [A-Z]+[a-z]* [A-Z]+[a-z]* b [A-Z]+[a-z]* [A-Z]+[a-z]*|c [A-Z]+[a-z]* [A-Z]+[a-z]* b [A-Z]+[a-z]*|c [A-Z]+[a-z]* b [A-Z]+[a-z]*|lbw b [A-Z]+[a-z]*|b [A-Z]+[a-z]*|not out|st [A-Z]+[a-z]* b [A-Z]+[a-z]*|run out \([A-Z]+[a-z]*\)|run out \([A-Z]+[a-z]*/[A-Z]+[a-z]*\))',read)
            mint=re.search(r'min \([0-9][0-9]?[0-9]?m\)',read) 

            if run.group()=='out':
                if mint:
                    act_min=re.search(r'\.*[0-9][0-9]?[0-9]?',mint.group())
            
            #print(dismissal)
            if i==batsman.group():
                noball_run=re.search(r'no ball, (\d?|FOUR|SIX)',run.group())
                if noball_run:
                    noball_=re.search(r'(\d|FOUR|SIX)',noball_run.group())
                if noball_run:
                    if noball_:
                        if noball_.group()=='1':
                            player_run=player_run+1
                            player_bowl+=1
                        elif noball_.group()=='2':
                            player_run=player_run+2
                            player_bowl+=1
                        elif noball_.group()=='3':
                            player_run=player_run+3
                            player_bowl+=1
                        elif noball_.group()=='4':
                            player_run=player_run+4
                            player_bowl+=1
                            player_4s=player_4s+1
                        elif noball_.group()=='6':
                            player_run=player_run+6
                            player_bowl+=1
                            player_6s=player_6s+1
                    else:
                        player_bowl+=1
                
                if run.group()=='FOUR':
                    player_run=player_run+4
                    player_bowl=player_bowl+1
                    player_4s=player_4s+1
                elif run.group()=='SIX':
                    player_run=player_run+6
                    player_bowl=player_bowl+1                
                    player_6s=player_6s+1
                elif run.group()=='1':
                    player_run=player_run+1
                    player_bowl=player_bowl+1
                elif run.group()=='2':
                    player_run=player_run+2
                    player_bowl=player_bowl+1
                elif run.group()=='3':
                    player_run=player_run+3
                    player_bowl=player_bowl+1
                elif run.group()=='4':
                    player_run=player_run+4
                    player_bowl=player_bowl+1
                elif run.group()=='5':
                    player_run=player_run+5
                    player_bowl=player_bowl+1
                elif run.group()=='no run':
                    player_run=player_run+0
                    player_bowl=player_bowl+1
                elif run.group()=='leg byes':
                    player_run=player_run+0
                    player_bowl=player_bowl+1
                elif run.group()=='Byes':
                    player_run=player_run+0
                    player_bowl=player_bowl+1
                elif run.group()=='out':
                    player_bowl=player_bowl+1
                    if bat_to_bowl:
                        dis=dismissal.group()
                        mn=act_min.group()

        player_stats_list.append(dis)
        player_stats_list.append(str(player_run))
        player_stats_list.append(str(player_bowl))
        player_stats_list.append(player_4s)
        player_stats_list.append(str(player_6s))
        if player_bowl==0:
            player_stats_list.append(0)  
        else:
            player_stats_list.append(player_run/player_bowl*100)
        player_stats_list.append(str(mn))

        bat_dict[i]=player_stats_list
        
        file.close()
    
    ########### calc of total run and total wicket
    try:
        file=open(fname,'r')
        read=file.readline()
        read=file.readline()
        for line in file:
            d=d+1
            read=file.readline()
            bat_to_bowl=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|[A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d?|Byes, \d?)',read)
            if bat_to_bowl:
                run=re.search(r'([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d?|Byes, \d?)',bat_to_bowl.group())
                #print(run.group())
                noball_run=re.search(r'no ball, \d?',run.group())
                legbyes_run=re.search(r'leg byes, \d?',run.group())
                byes_run=re.search(r'Byes, \d?',run.group())
                wide_run=re.search(r'wide, \d?',run.group())

            if noball_run:
                noball_=re.search(r'\d',noball_run.group())
            if legbyes_run:
                legbyes_=re.search(r'\d',legbyes_run.group())
            if byes_run:
                byes_=re.search(r'\d',byes_run.group())
            if wide_run:
                wide_=re.search(r'\d',wide_run.group())
            if noball_run:
                if noball_:
                    if noball_.group()=='1':
                        Totrun+=2
                    elif noball_.group()=='2':
                        Totrun+=3
                    elif noball_.group()=='3':
                        Totrun+=4
                    elif noball_.group()=='4':
                        Totrun+=5
                    elif noball_.group()=='6':
                        Totrun+=7
                else:
                    Totrun+=1
            if wide_run:
                if wide_:
                    if wide_.group()=='1':
                        Totrun+=1
                    elif wide_.group()=='2':
                        Totrun+=2
                    elif wide_.group()=='3':
                        Totrun+=3
                    elif wide_.group()=='5':
                        Totrun+=5
                else:
                    Totrun+=1
            if legbyes_run:
                if legbyes_:
                    if legbyes_.group()=='1':
                        Totrun+=1
                    elif legbyes_.group()=='2':
                        Totrun+=2
                    elif legbyes_.group()=='3':
                        Totrun+=3
                    elif legbyes_.group()=='4':
                        Totrun+=4
                    elif legbyes_.group()=='6':
                        Totrun+=6
                else:
                    Totrun+=0
            if byes_run:
                if byes_:
                    if byes_.group()=='1':
                        Totrun+=1
                    elif byes_.group()=='2':
                        Totrun+=2
                    elif byes_.group()=='3':
                        Totrun+=3
                    elif byes_.group()=='4':
                        Totrun+=4
                    elif byes_.group()=='6':
                        Totrun+=6
                else:
                    Totrun+=0
            if run.group()=='FOUR':
                Totrun=Totrun+4
            elif run.group()=='SIX':
                Totrun=Totrun+6
            elif run.group()=='wide':
                Totrun=Totrun+1
            elif run.group()=='1':
                Totrun=Totrun+1
            elif run.group()=='2':
                Totrun=Totrun+2
            elif run.group()=='3':
                Totrun=Totrun+3
            elif run.group()=='4':
                Totrun=Totrun+4
            elif run.group()=='5':
                Totrun=Totrun+5
            elif run.group()=='6':
                Totrun=Totrun+6
            elif run.group()=='out':
                wicket=wicket+1
    except:
        pass

    ##################################
    #print(bat_dict)
    ############calc batsman scorecard
    #print("----------------------------------------------------------------------------------------------------------------------------------------")
    #print('\t\t\t\t\t2nd Innings')
    #print("----------------------------------------------------------------------------------------------------------------------------------------\n")
    b="BATSMAN"
    r="R"
    ba="B"
    fours="4s"
    sixes="6s"
    SR="SR"
    #print("%-56s %-8s %-8s %-8s %-8s %-8s %-8s" %(b,r,ba,'M',fours,sixes,SR))
    file1.writelines("%-56s %-8s %-8s %-8s %-8s %-8s %-8s" %(b,r,ba,'M',fours,sixes,SR))
    #print("")
    file1.writelines("\n\n")

    for i in played_bats:
        pass
        #print("%-20s %-35s %-8s %-8s %-8s %-8s %-8s %.2f" %(i,str(bat_dict[i][0]),str(bat_dict[i][1]),str(bat_dict[i][2]),str(bat_dict[i][6]),str(bat_dict[i][3]),str(bat_dict[i][4]),bat_dict[i][5]))
        file1.writelines("%-20s %-35s %-8s %-8s %-8s %-8s %-8s %.2f" %(i,str(bat_dict[i][0]),str(bat_dict[i][1]),str(bat_dict[i][2]),str(bat_dict[i][6]),str(bat_dict[i][3]),str(bat_dict[i][4]),bat_dict[i][5]))
        file1.writelines('\n')
    file1.writelines('\n')

    file.close()
    ############### end of batsman scorecard


    ##### extras calculation
    try:
            
        file=open(fname,'r')
        read=file.readline()
        read=file.readline()
        byes=0
        legbyes=0
        wide=0
        noball=0
        for line in file:
            read=file.readline()
            bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d|Byes, \d)',read)
            #print(bat_to_bowl.group())
            if bat_to_bowl:
                extras_byes=re.search(r'\.*, Byes, \d',bat_to_bowl.group())
                extras_legbyes=re.search(r'\.*, leg byes, \d',bat_to_bowl.group())
                extras_wide=re.search(r'\.*, wide, \d?',bat_to_bowl.group())
                extras_noball=re.search(r'\.*, no ball, \d?',bat_to_bowl.group())

            if extras_byes:
                extras_byes_run=re.search(r'\d',extras_byes.group())

            if extras_legbyes:
                extras_legbyes_run=re.search(r'\d',extras_legbyes.group())

            if extras_wide:
                extras_wide_run=re.search(r'\d',extras_wide.group())

            if extras_noball:
                extras_noball_run=re.search(r'\d',extras_noball.group())

            if extras_byes:
                if extras_byes_run:
                    if extras_byes_run.group()=='1':
                        byes+=1
                    elif extras_byes_run.group()=='2':
                        byes+=2
                    elif extras_byes_run.group()=='3':
                        byes+=3
                    elif extras_byes_run.group()=='4':
                        byes+=4
                    elif extras_byes_run.group()=='5':
                        byes+=5
            if extras_legbyes:
                if extras_legbyes_run:
                    if extras_legbyes_run.group()=='1':
                        legbyes+=1
                    elif extras_legbyes_run.group()=='2':
                        legbyes+=2
                    elif extras_legbyes_run.group()=='3':
                        legbyes+=3
                    elif extras_legbyes_run.group()=='4':
                        legbyes+=4
                    elif extras_legbyes_run.group()=='5':
                        legbyes+=5
            if extras_wide:
                if extras_wide_run:
                    if extras_wide_run.group()=='2':
                        wide+=2
                    elif extras_wide_run.group()=='3':
                        wide+=3
                    elif extras_wide_run.group()=='4':
                        wide+=4
                    elif extras_wide_run.group()=='5':
                        wide+=5 
                else :
                    wide+=1    
            if extras_noball:
                if extras_noball_run:
                        noball+=1
                else :
                    noball+=1

        extras = byes + legbyes + wide + noball
        ex="Extras"
        #print('')
        #print("%-56s %s %s %s %s %s %s %s %s %s %s %s"%(ex, extras,'(','b',byes,'lb',legbyes,'w',wide,'nb',noball,')'))
        file1.writelines("%-56s %s %s %s %s %s %s %s %s %s %s %s"%(ex, extras,'(','b',byes,'lb',legbyes,'w',wide,'nb',noball,')'))
        file1.writelines('\n')
        file.close()
    except:
        pass
    ##############################

    ########## team score
    try:
        file=open(fname,'r')
        ballleft=0
        total_ball=0
        overs=0
        ballleft=0
        RunRate = 0
        lines = file.read().splitlines()
        if os.stat(fname).st_size :
            try:
                overs = float(lines[-1])
            except:
                overs = float(lines[-2])
        #overs=overs.rstrip('\n')
        if overs=='49.6':
            overs=50.0

        total_ball= (total_ball-2)/2-wide-noball
        if float(overs):
            RunRate=Totrun/float(overs)

        if wicket==11:
            wicket=10
        #print("%-56s %s%s%s %s %s %s %s %s %.2f %s"%('Total',str(Totrun),'/',str(wicket),'(',overs,'Overs','RR:','',RunRate ,')'))
        file1.writelines("%-56s %s%s%s %s %s %s %s %s %.2f %s"%('Total',str(Totrun),'/',str(wicket),'(',overs,'Overs','RR:','',RunRate ,')'))
        file1.writelines('\n')
        file.close()


        ###############did not bat & fall
        #print('Did not Bat',end='')
        file1.writelines('Did not Bat')
        file1.writelines('\t\t\t\t\t\t\t\t\t')
        #print('%-20s'%(''),end='')
        for didnot in list(set(playing11_ing2[0]).difference(set(played_bats))):
            #print(didnot+',',end='')
            file1.writelines(didnot+',')
        #print('')
        file1.writelines('\n')
        #print('Fall of Wickets : ',end='')
        file1.writelines('Fall of Wickets : ')

        for i in played_bats:
            fall_list=[]
            file=open(fname,'r')
            read=file.readline()
            read=file.readline()
            for line in file:
                d=d+1
                read=file.readline()
                bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball, \d?|no run|wide, \d?|leg byes, \d|Byes, \d)',read)
                if bat_to_bowl:
                    batsman_to=re.search(r'(\.*to [A-Z]+[a-z]* [A-Z]+[a-z]*|\.*to [A-Z]+[a-z]*)',bat_to_bowl.group())
                    batsman=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|\.*[A-Z]+[a-z]*)',batsman_to.group())    
                fall_wkt=re.search(r'wkt \([0-9]0?-[0-9][0-9]?[0-9]?\) ',read)

                if fall_wkt:
                    act_wkt=re.search(r'[0-9]0?-[0-9][0-9]?[0-9]?',fall_wkt.group())
                if i==batsman.group():
                    if fall_wkt:
                        line_notn=line.rstrip('\n')
                        fall_list.append(line_notn)
                        fall_list.append(act_wkt.group())
            if bat_dict[i][0]!='not out':
                fall[i]=fall_list
        #print(fall)
        file.close()
        #print(len(fall))
        k=0
        for i in played_bats:
            if bat_dict[i][0]!='not out':
                k+=1
                if k==len(fall)+1:
                    break
                #print(fall[i][1]+' ('+i+','+fall[i][0]+' ov), ',end='')
                file1.writelines(fall[i][1]+' ('+i+','+fall[i][0]+' ov), ')
        #print('')
        file1.write("\n\n")
        fall={}
        #####################################
    except:
        pass

    ############__bowling list__##

    try:
        file=open(fname,'r')
        read=file.readline()
        read=file.readline()
        for line in file:
            d=d+1
            read=file.readline()
            bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,)',read)
            if bat_to_bowl:
                bowler=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|[A-Z]+[a-z]*)',bat_to_bowl.group())
                #print(bowler.group())
                bname=bowler.group()
                if bname not in played_bowl:
                    played_bowl.append(bname)

        file.close()
        ###########bowl list done
    except:
        pass    
    #########__bowl stats calc___

    for i in played_bowl:
        player_stats_list=[]
        player_ball=0
        player_bowl_run=0
        player_wkts=0
        player_nb=0
        player_wd=0
        player_mdn=0
        bowler_4s=0
        bowler_6s=0
        bowler_0s=0
        file=open(fname,'r')
        read=file.readline()
        read=file.readline()
        for line in file:
            read=file.readline()
            bat_to_bowl=re.search(r'([A-Z]+[a-z]*|[A-Z]+[a-z]* [A-Z]+[a-z]*) to [A-Z]+[a-z]*(,| [A-Z]+[a-z]*,) ([0-6]|FOUR|SIX|out|no ball|no run|wide|leg)',read)
            dismissal = re.search(r'run out \([A-Z]+[a-z]*\)|run out \([A-Z]+[a-z]*/[A-Z]+[a-z]*\)',read)
            if dismissal:
                runoutdis=re.search(r'run out',dismissal.group())
            if bat_to_bowl:
                bowler=re.search(r'([A-Z]+[a-z]* [A-Z]+[a-z]*|[A-Z]+[a-z]*)',bat_to_bowl.group())
                run=re.search(r'([0-6]|FOUR|SIX|out|no ball|no run|wide|leg)',bat_to_bowl.group())
            if i==bowler.group():
                if run.group()=='FOUR':
                    player_bowl_run+=4
                    player_ball+=1
                    bowler_4s+=1
                elif run.group()=='SIX':
                    player_bowl_run+=6
                    player_ball+=1
                    bowler_6s+=1
                elif run.group()=='1':
                    player_bowl_run+=1
                    player_ball+=1
                elif run.group()=='2':
                    player_bowl_run+=2
                    player_ball+=1
                elif run.group()=='3':
                    player_bowl_run+=3
                    player_ball+=1
                elif run.group()=='4':
                    player_bowl_run+=4
                    player_ball+=1
                elif run.group()=='5':
                    player_bowl_run+=4
                    player_ball+=1
                elif run.group()=='no run':
                    player_bowl_run+=0
                    player_ball+=1
                    bowler_0s+=1
                elif run.group()=='out':
                    player_bowl_run+=0
                    player_ball+=1
                    player_wkts+=1
                elif run.group()=='no ball':
                    player_bowl_run+=1
                    player_ball+=0
                    player_nb+=1
                elif run.group()=='wide':
                    player_bowl_run+=1
                    player_ball+=0
                    player_wd+=1
                elif run.group()=='leg':
                    player_bowl_run+=0
                    player_ball+=1
                if bowler.group()=='Bumrah':
                    player_mdn=1
                if dismissal:
                    if runoutdis:
                        player_wkts-=1
        ovrs=int(player_ball/6)
        left=int(player_ball%6)
        tot=str(ovrs)+'.'+str(left)
        player_stats_list.append(tot)
        player_stats_list.append(player_mdn)
        player_stats_list.append(str(player_bowl_run))
        player_stats_list.append(str(player_wkts))
        player_stats_list.append(player_bowl_run/player_ball*6)
        player_stats_list.append(bowler_0s)
        player_stats_list.append(bowler_4s)
        player_stats_list.append(bowler_6s)
        player_stats_list.append(str(player_wd))
        player_stats_list.append(str(player_nb))

        
        bowl_dict[i]=player_stats_list
        
        #print("run scored by "+i+' '+dis+' '+str(player_run)+' '+str(player_bowl)+' '+str(player_4s)+' '+str(player_6s)+' '+str(player_run/player_bowl*100))
        file.close()
    #print(played_bowl)
    #print("--------------------------------------------------------------------------------------------------------------------------------------")
    #print('')
    #print("%-50s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s" %('BOWLER','O','M','R','W','ECON','0s','4s','6s','WD','NB'))
    file1.writelines("%-50s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s" %('BOWLER','O','M','R','W','ECON','0s','4s','6s','WD','NB'))
    #print('')
    file1.writelines('\n\n')

    for i in played_bowl:
        #pass
        #print("%-50s %-8s %-8s %-8s %-8s %.2f %-3s %-8s %-8s %-8s %-8s %-8s" %(i,str(bowl_dict[i][0]),str(bowl_dict[i][1]),str(bowl_dict[i][2]),str(bowl_dict[i][3]),bowl_dict[i][4],'',str(bowl_dict[i][5]),str(bowl_dict[i][6]),str(bowl_dict[i][7]),str(bowl_dict[i][8]),str(bowl_dict[i][9])))
        file1.writelines("%-50s %-8s %-8s %-8s %-8s %.2f %-3s %-8s %-8s %-8s %-8s %-8s" %(i,str(bowl_dict[i][0]),str(bowl_dict[i][1]),str(bowl_dict[i][2]),str(bowl_dict[i][3]),bowl_dict[i][4],'',str(bowl_dict[i][5]),str(bowl_dict[i][6]),str(bowl_dict[i][7]),str(bowl_dict[i][8]),str(bowl_dict[i][9])))
        file1.writelines('\n')


    file.close()

    file1.close()


    #print('\n\nThis Computed Scorecard also saved in file "roll_MatchID_scorecard_computed.txt" in folder "Computed_scorecard" \n\n')

