$hostnames = "<Your_Hostnames>"  # Define hostnames as needed, e.g., "Host1,Host2"
$csvpath = "<Your_CSV_Path>"  # Path to save the CSV file, e.g., "C:\path\to\log.csv"

function Get-NetworkStatistics {
    # Function details omitted for brevity
    <#
    .SYNOPSIS
        Display current TCP/IP connections for local or remote system
    .DESCRIPTION
        Display current TCP/IP connections for local or remote system. Includes the process ID (PID) and process name for each connection.
    #>
    [OutputType('System.Management.Automation.PSObject')]
    [CmdletBinding()]
    param(
        [Parameter(Position=0)]
        [System.String]$ProcessName='*',
        
        [Parameter(Position=1)]
        [System.String]$Address='*',        
        
        [Parameter(Position=2)]
        $Port='*',

        [Parameter(Position=3, ValueFromPipeline = $True, ValueFromPipelineByPropertyName = $True)]
        [System.String[]]$ComputerName=$env:COMPUTERNAME,

        [ValidateSet('*','tcp','udp')]
        [System.String]$Protocol='*',

        [ValidateSet('*','Closed','Close_Wait','Closing','Delete_Tcb','DeleteTcb','Established','Fin_Wait_1','Fin_Wait_2','Last_Ack','Listening','Syn_Received','Syn_Sent','Time_Wait','Unknown')]
        [System.String]$State='*',

        [switch]$ShowHostnames,
        
        [switch]$ShowProcessNames = $true,    

        [System.String]$TempFile = "C:\netstat.txt",

        [validateset('*','IPv4','IPv6')]
        [string]$AddressFamily = '*'
    )
    
    begin {
        $properties = 'ComputerName','Protocol','LocalAddress','LocalPort','RemoteAddress','RemotePort','State','ProcessName','PID'
        $dnsCache = @{}
    }
    
    process {
        foreach($Computer in $ComputerName) {
            if($ShowProcessNames) {
                Try {
                    $processes = Get-Process -ComputerName $Computer -ErrorAction stop | select name, id
                } Catch {
                    Write-warning "Could not retrieve process names. Defaulting to no ShowProcessNames."
                    $ShowProcessNames = $false
                }
            }

            # Handle local and remote systems
            if($Computer -ne $env:COMPUTERNAME) {
                $cmd = "cmd /c c:\windows\system32\netstat.exe -ano >> $TempFile"
                $remoteTempFile = "\\$Computer\c$\netstat.txt"

                Try {
                    $null = Invoke-WmiMethod -class Win32_process -name Create -ArgumentList "cmd /c del $TempFile" -ComputerName $Computer -ErrorAction stop
                } Catch {
                    Write-Warning "Could not delete previous results on $Computer."
                }

                Try {
                    $processID = (Invoke-WmiMethod -class Win32_process -name Create -ArgumentList $cmd -ComputerName $Computer -ErrorAction stop).processid
                } Catch {
                    Throw $_
                }

                while (Get-Process -Id $processID -ComputerName $Computer -ErrorAction SilentlyContinue) {
                    Start-Sleep -Seconds 2
                }

                if(Test-Path $remoteTempFile) {
                    $results = Get-Content $remoteTempFile | Select-String -Pattern '\s+(TCP|UDP)'
                    Remove-Item $remoteTempFile -Force
                } else {
                    Throw "Path $TempFile on $Computer is not accessible."
                }
            } else {
                $results = netstat -ano | Select-String -Pattern '\s+(TCP|UDP)'
            }

            $totalCount = $results.Count
            $count = 0

            foreach($result in $results) {
                $item = $result.Line.Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries)

                # Parsing logic for network data
                # Additional filtering and processing here

                # Construct the object for each connection
                New-Object -TypeName PSObject -Property @{
                    ComputerName = $Computer
                    PID = $procId
                    ProcessName = $procName
                    Protocol = $proto
                    LocalAddress = $localAddress
                    LocalPort = $localPort
                    RemoteAddress = $remoteAddress
                    RemotePort = $remotePort
                    State = $status
                } | Select-Object -Property $properties

                $count++
            }
        }
    }
}

$hostnamesArr = $hostnames.Split(",")
while ($true) {
    foreach ($hostname in $hostnamesArr) {    
        Write-Host "Checking host:" $hostname
        $table = @()
        $table += (Import-Csv $csvpath | Select-Object "A_ComputerName","B_LocalAddress","C_LocalPort","D_RemoteAddress","E_RemotePort","F_State","G_ProcessName","H_Protocol","I_Count","J_DateAdded")
        $netstat = Get-NetworkStatistics -ComputerName $hostname -State Established -Protocol tcp -ShowHostnames 

        foreach ($entry in $netstat) {
            $found = $false
            foreach ($row in $table) {
                if ($row.B_LocalAddress -eq $entry.LocalAddress -and $row.C_LocalPort -eq $entry.LocalPort -and $row.D_RemoteAddress -eq $entry.RemoteAddress -and $row.E_RemotePort -eq $entry.RemotePort) {
                    $found = $true
                    $row.I_COUNT = [int]$row.I_COUNT + 1
                }
            }
            if (-not $found) {
                $csvObject = New-Object PSObject -Property @{
                    'A_ComputerName' = $entry.ComputerName
                    'B_LocalAddress' = $entry.LocalAddress
                    'C_LocalPort' = $entry.LocalPort
                    'D_RemoteAddress' = $entry.RemoteAddress
                    'E_RemotePort' = $entry.RemotePort
                    'F_State' = $entry.State
                    'G_ProcessName' = $entry.ProcessName
                    'H_Protocol' = $entry.Protocol
                    'I_Count' = 1
                    'J_DateAdded' = Get-Date
                }
                $table += $csvObject | Select-Object "A_ComputerName","B_LocalAddress","C_LocalPort","D_RemoteAddress","E_RemotePort","F_State","G_ProcessName","H_Protocol","I_Count","J_DateAdded"
            }
        }
        $table | Export-Csv $csvpath -NoTypeInformation 
    }
}
