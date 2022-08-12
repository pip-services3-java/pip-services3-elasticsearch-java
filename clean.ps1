#!/usr/bin/env pwsh

# Get component metadata and set necessary variables
$component = Get-Content -Path "$PSScriptRoot/component.json" | ConvertFrom-Json
$buildImage = "$($component.registry)/$($component.name):$($component.version)-$($component.build)-build"
$docsImage = "$($component.registry)/$($component.name):$($component.version)-$($component.build)-docs"
$testImage = "$($component.registry)/$($component.name):$($component.version)-$($component.build)-test"

# Remove docker images
docker rmi $buildImage --force
docker rmi $docsImage --force
docker rmi $testImage --force
docker rmi -f $(docker images -f "dangling=true" -q) # remove build container if build fails
docker image prune --force

# Remove existed containers
$exitedContainers = docker ps -a | Select-String -Pattern "Exit"
foreach($c in $exitedContainers) { docker rm $c.ToString().Split(" ")[0] }

# Remove unused volumes
docker volume rm -f $(docker volume ls -f "dangling=true")

# Clean up build directories
if (Test-Path -Path "$PSScriptRoot/obj") {
    Remove-Item -Recurse -Force "$PSScriptRoot/obj"
}
if (Test-Path -Path "$PSScriptRoot/lib") {
    Remove-Item -Recurse -Force "$PSScriptRoot/lib"
}