from utils.console.io import IO


_MAIN_BANNER = r"""{}
                                                                                                                    
 __               __                    __   ___     __   __  ___
/  \ |__/ \_/ __ |__) | |\ |  /\  |\ | /  ` |__  __ |__) /  \  | 
\__/ |  \ / \    |__) | | \| /~~\ | \| \__, |___    |__) \__/  | 
                                                
version: [_V_]
build: [_B_]
                  
""".format(IO.Fore.WHITE, IO.Style.RESET_ALL + IO.Style.BRIGHT)                                    

def get_main_banner(version, build, banner=_MAIN_BANNER):
    return banner.replace('[_V_]', version).replace('[_B_]', build)

def run_banner(version, build):
    """
    Main entry point of the application
    @fetch version from a function
    """
    IO.spacer()
    IO.print(get_main_banner(version, build, _MAIN_BANNER))
    IO.spacer()
