# Tobin-Bridge-VISSIM-Processing

Things to do before running the code:

1. Install Anaconda: install anaconda for managing python packages. Following is a link 
	to anaconda: https://www.anaconda.com/products/individual

2. Open "Anaconda Prompt". It is similar to "Command Prompt". Type "Ananconda Prompt" 
   in the start menu to open it. 

3. Create a new environment. An environment has a python interpreter and allows you to 
   install a set of python libraries.  Let's create an environment "env_py3". Type the 
   following command in "Anaconda Prompt": conda create -n env_py3 python=3.8.
	i. This will create a python 3.8 environment with base python libraries. 

4. Close "Anaconda Prompt". Now reopen "Anaconda Prompt". Open "env_py3" by typing: 
   activate env_py3

5. Install all the packages needed to run the scripts for tobin bridge by typing: 
   conda install --file "\\kittelson.com\fs\H_Projects\21\21410 - MassDOT Complete Streets On-Call\016 - Tobin Bridge Managed Lane Phase II\analysis\VISSIM\Tobin Bridge Vissim Processing\spec.txt"
	i. Ref: https://stackoverflow.com/questions/53742827/add-full-anaconda-package-list-to-existing-conda-environment

Note: Step 1 to 5 just need to be done only once. 

6. Close "Anaconda Prompt". Now reopen "Anaconda Prompt". Open "env_py3" by typing: 
   activate env_py3

7. Open Spyder by typing: spyder

8. Change the working directory in spyder to the project directory for the script. For 
   network drive this would be "\\kittelson.com\fs\H_Projects\21\21410 - MassDOT Complete Streets On-Call\016 - Tobin Bridge Managed Lane Phase II\analysis\VISSIM\Tobin Bridge Vissim Processing"
	- see the figure in the foloowing link: https://stackoverflow.com/questions/39905258/spyder-3-set-console-working-directory-not-working
	  The folder icon shown in the figure can be used to change the working directory. 
	
9. Run the processing scripts. 