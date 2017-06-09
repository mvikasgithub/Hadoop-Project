#!/bin/bash 
show_menu()
{
	echo  "========== BIG DATA ANALYSIS ON CENSUS DATA USING HADOOP ===================="
	echo  "1: Educational Analysis :"
	echo  "2: Financial Analysis :"
	echo  "3: Social Analysis :"
	echo  "4: Planning Analysis :"
	echo  "==========" 
	echo  "enter your option (Enter key to exit):"
    	read mainmenuopt
}

scriptchoicemenu()
{
	echo  "1: Hive Script :"
	echo  "2: Pig Script :"
	echo  "3: MapReduce Script :"
	echo  "Press enter to return to previous menu :"
	read retval

	return $retval
}

edumenuquery1()
{
	scriptchoicemenu
	local retval=$?
	echo $retval

	echo "Processing...... please wait...."
	if [[ $retval == 1 ]]; then	
		hive -e "select education,gender, count(*) from db1.census group by education, gender"
	elif [[ $retval == 2 ]]; then
		pig -f eq1-pig-edu-gender
	elif [[ $retval == 3 ]]; then
		hadoop jar eq1-EducationGenderCount.jar /user/hadoop/census.txt /user/hadoop/op
		hadoop fs -cat /user/hadoop/op/p*
	else
		return
	fi

	echo "Press any key to continue...."
	read waitforresulttobeseen
}

edumenuquery2()
{
	scriptchoicemenu
	local retval=$?
	echo $retval

	echo "Processing...... please wait...."
	if [[ $retval == 1 ]]; then	
		hive -e "select education,weeksworked, count(*) from db1.census group by education, weeksworked;"
	elif [[ $retval == 2 ]]; then
		pig -f eq1-pig-edu-gender
	elif [[ $retval == 3 ]]; then
		echo "MapReduce solution for this not available...."
	else
		return
	fi

	echo "Press any key to continue...."
	read waitforresulttobeseen
}

edumenuquery3()
{
	scriptchoicemenu
	local retval=$?
	echo $retval

	echo "Processing...... please wait...."
	if [[ $retval == 1 ]]; then	
		hive -e "select gender, count(*) from db1.census group by gender;"
	elif [[ $retval == 2 ]]; then
		pig -f eq3-pig-sex-ratio
	elif [[ $retval == 3 ]]; then
		hadoop jar eq3-SexRatio.jar /user/hadoop/census.txt /user/hadoop/op
		hadoop fs -cat /user/hadoop/op/p*
	else
		return
	fi

	echo "Press any key to continue...."
	read waitforresulttobeseen
}

finmenuquery1()
{
	scriptchoicemenu
	local retval=$?
	echo $retval

	echo "Processing...... please wait...."
	if [[ $retval == 1 ]]; then	
		hive -e "select taxfilerstatus, sum(income) from db1.census where taxfilerstatus not like '%Nonfiler%' group by taxfilerstatus;"
	elif [[ $retval == 2 ]]; then
		pig -f fin1-pig-income-taxfilerstatus
	elif [[ $retval == 3 ]]; then
		echo "MapReduce solution for this not available...."	
	else
		return
	fi

	echo "Press any key to continue...."
	read waitforresulttobeseen
}

finmenuquery2()
{
	scriptchoicemenu
	local retval=$?
	echo $retval

	echo "Processing...... please wait...."
	if [[ $retval == 1 ]]; then	
		hive -e "select gender, sum(income) from db1.census group by gender;"
	elif [[ $retval == 2 ]]; then
		pig -f fin2-pig-genderwiseincome
	elif [[ $retval == 3 ]]; then
		echo "MapReduce solution for this not available...."	
	else
		return
	fi

	echo "Press any key to continue...."
	read waitforresulttobeseen
}

finmenuquery3()
{
	scriptchoicemenu
	local retval=$?
	echo $retval

	echo "Processing...... please wait...."
	if [[ $retval == 1 ]]; then	
		hive -e "select count(taxfilerstatus) from db1.census where taxfilerstatus not like '%%Nonfiler%%';"
	elif [[ $retval == 2 ]]; then
		pig -f fin3-pig-totaltaxpayers
	elif [[ $retval == 3 ]]; then
		hadoop jar fin3-TotalTaxPayers.jar /user/hadoop/census.txt /user/hadoop/op
		hadoop fs -cat /user/hadoop/op/p*
	else
		return
	fi

	echo "Press any key to continue...."
	read waitforresulttobeseen
}

finmenuquery4()
{
	scriptchoicemenu
	local retval=$?
	echo $retval

	echo "Processing...... please wait...."
	if [[ $retval == 1 ]]; then	
		echo "Hive Solution for this not available...."
	elif [[ $retval == 2 ]]; then
		echo "Pig Solution for this not available...."
	elif [[ $retval == 3 ]]; then
		hadoop jar fin4-TotalTaxToBeCollected.jar /user/hadoop/census.txt /user/hadoop/op
		hadoop fs -cat /user/hadoop/op/p*
	else
		return
	fi

	echo "Press any key to continue...."
	read waitforresulttobeseen
}

finmenuquery5()
{
	scriptchoicemenu
	local retval=$?
	echo $retval

	echo "Processing...... please wait...."
	if [[ $retval == 1 ]]; then	
		hive -e "select sum(income) / count(*) from db1.census;"
	elif [[ $retval == 2 ]]; then
		echo "Pig Solution for this not available...."
	elif [[ $retval == 3 ]]; then
		echo "MapReduce solution for this not available...."
	else
		return
	fi

	echo "Press any key to continue...."
	read waitforresulttobeseen
}


finmenuquery6()
{
	scriptchoicemenu
	local retval=$?
	echo $retval

	echo "Processing...... please wait...."
	if [[ $retval == 1 ]]; then	
		echo "For Male: "
		hive -e "select gender, sum(income)/count(*) from db1.census where gender = ' Male' group by gender;"
		echo "Press any key to continue....."
		read waitforresulttobeseen
		echo "For Female: "
		hive -e "select gender, sum(income)/count(*) from db1.census where gender = ' Female' group by gender;"
		
	elif [[ $retval == 2 ]]; then
		echo "Pig Solution for this not available...."
	elif [[ $retval == 3 ]]; then
		hadoop jar fin6-GenderWisePerCapitaIncome.jar /user/hadoop/census.txt /user/hadoop/op
		hadoop fs -cat /user/hadoop/op/p*
	else
		return
	fi

	echo "Press any key to continue...."
	read waitforresulttobeseen
}


eduanaysismenu()
{
	echo "<=================EDUCATIONAL ANALYSIS=============>"	
	echo " Query 1: Education Qualification Count : Sub-grouped by Gender"
	echo " Query 2: Education Qualification Count based on Employment"
	echo " Query 3: Calculate Sex Ratio (Male : Female)"
	echo  "==========" 
	echo  "Enter your option (Enter key to go to previous menu):"
	read edumenuopt

	clear
	while [[ $edumenuopt != "" ]]
		do
	    		if [[ $edumenuopt == "" ]];then 
				return ""
				
			else
				case $edumenuopt in
				1)	edumenuquery1;
					;;
				2)	edumenuquery2;
					;;
				3)	edumenuquery3;
					;;
				esac
					eduanaysismenu;

			fi
		done
}


financialanalysismenu()
{
	echo "<=================EDUCATIONAL ANALYSIS=============>"	
	echo " Query 1: Total Income of different types of Tax Payers"
	echo " Query 2: Gender wise Total Income Generated"
	echo " Query 3: Total Taxpayers"
	echo " Query 4: Total Tax To Be Collected"
	echo " Query 5: Per Capita Income"
	echo " Query 6: Gender wise Per Capita Income"
	echo  "==========" 
	echo  "Enter your option (Enter key to go to previous menu):"
	read finmenuopt

	clear
	while [[ $finmenuopt != "" ]]
		do
	    		if [[ $finmenuopt == "" ]];then 
				return ""
				
			else
				case $finmenuopt in
				1)	finmenuquery1;
					;;
				2)	finmenuquery2;
					;;
				3)	finmenuquery3;
					;;
				4)	finmenuquery4;
					;;
				5)	finmenuquery5;
					;;
				6)	finmenuquery6;
					;;
				esac
					financialanalysismenu;

			fi
		done
}

clear
show_menu
while [[ $mainmenuopt != "" ]]
	do
    		if [[ $mainmenuopt = "" ]]; then 
	            exit;
		else
			case $mainmenuopt in
			1)	clear;
				eduanalysismenu;
				show_menu;
				;;
			2)	clear;
				financialanalysismenu;
				show_menu;
				;;
			3)	clear;
				echo "<=================SOCIAL ANALYSIS=============>"
				show_menu;
				;;
			4)	clear;
				echo "<=================PLANNING ANALYSIS=============>"
				show_menu;
				;;
			esac
		fi
	done
