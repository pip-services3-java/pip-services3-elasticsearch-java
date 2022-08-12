#!/usr/bin/env pwsh

Set-StrictMode -Version latest
$ErrorActionPreference = "Stop"

# Get component metadata and set necessary variables
$component = Get-Content -Path "$PSScriptRoot/component.json" | ConvertFrom-Json
$buildImage = "$($component.registry)/$($component.name):$($component.version)-$($component.build)-build"
$container = $component.name

# Remove build files
if (Test-Path -Path "$PSScriptRoot/obj") {
    Remove-Item -Recurse -Force -Path "$PSScriptRoot/obj"
}
if (Test-Path -Path "$PSScriptRoot/lib") {
    Remove-Item -Recurse -Force -Path "$PSScriptRoot/lib"
}

# Build docker image
docker build -f "$PSScriptRoot/docker/Dockerfile.build" -t $buildImage .

# Create and copy compiled files, then destroy
docker create --name $container $buildImage
docker cp "$($container):/app/obj" "$PSScriptRoot/obj"
docker cp "$($container):/app/lib" "$PSScriptRoot/lib"
docker rm $container

# Verify build
if (-not (Test-Path -Path "$PSScriptRoot/obj") -or -not (Test-Path -Path "$PSScriptRoot/lib")) {
    Write-Error "obj or lib folder doesn't exists in root dir. Build failed. See logs above for more information."
}
