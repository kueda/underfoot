#!/bin/sh
# based on https://gist.github.com/Lerg/b0a643a13f751747976f
# base=$1

base="resources/icon.png"

# ICONS

echo "resources/ios/icon/icon-small.png"
convert "$base" -resize '29x29'     -unsharp 1x4 "resources/ios/icon/icon-small.png"
echo "resources/ios/icon/icon-small-40.png"
convert "$base" -resize '40x40'     -unsharp 1x4 "resources/ios/icon/icon-small-40.png"
echo "resources/ios/icon/icon-small-50.png"
convert "$base" -resize '50x50'     -unsharp 1x4 "resources/ios/icon/icon-small-50.png"
echo "resources/ios/icon/icon.png"
convert "$base" -resize '57x57'     -unsharp 1x4 "resources/ios/icon/icon.png"
echo "resources/ios/icon/icon-small@2x.png"
convert "$base" -resize '58x58'     -unsharp 1x4 "resources/ios/icon/icon-small@2x.png"
echo "resources/ios/icon/icon-60.png"
convert "$base" -resize '60x60'     -unsharp 1x4 "resources/ios/icon/icon-60.png"
echo "resources/ios/icon/icon-72.png"
convert "$base" -resize '72x72'     -unsharp 1x4 "resources/ios/icon/icon-72.png"
echo "resources/ios/icon/icon-76.png"
convert "$base" -resize '76x76'     -unsharp 1x4 "resources/ios/icon/icon-76.png"
echo "resources/ios/icon/icon-small-40@2x.png"
convert "$base" -resize '80x80'     -unsharp 1x4 "resources/ios/icon/icon-small-40@2x.png"
echo "resources/ios/icon/icon-small-50@2x.png"
convert "$base" -resize '100x100'   -unsharp 1x4 "resources/ios/icon/icon-small-50@2x.png"
echo "resources/ios/icon/icon@2x.png"
convert "$base" -resize '114x114'   -unsharp 1x4 "resources/ios/icon/icon@2x.png"
echo "resources/ios/icon/icon-60@2x.png"
convert "$base" -resize '120x120'   -unsharp 1x4 "resources/ios/icon/icon-60@2x.png"
echo "resources/ios/icon/icon-72@2x.png"
convert "$base" -resize '144x144'   -unsharp 1x4 "resources/ios/icon/icon-72@2x.png"
echo "resources/ios/icon/icon-76@2x.png"
convert "$base" -resize '152x152'   -unsharp 1x4 "resources/ios/icon/icon-76@2x.png"
echo "resources/ios/icon/icon-60@3x.png"
convert "$base" -resize '180x180'   -unsharp 1x4 "resources/ios/icon/icon-60@3x.png"
# TODO
# convert "$base" -resize '512x512'   -unsharp 1x4 "resources/ios/icon/iTunesArtwork"
# convert "$base" -resize '1024x1024' -unsharp 1x4 "resources/ios/icon/iTunesArtwork@2x"
echo "resources/android/icon/drawable-ldpi-icon.png"
convert "$base" -resize '36x36'     -unsharp 1x4 "resources/android/icon/drawable-ldpi-icon.png"
echo "resources/android/icon/drawable-mdpi-icon.png"
convert "$base" -resize '48x48'     -unsharp 1x4 "resources/android/icon/drawable-mdpi-icon.png"
echo "resources/android/icon/drawable-hdpi-icon.png"
convert "$base" -resize '72x72'     -unsharp 1x4 "resources/android/icon/drawable-hdpi-icon.png"
echo "resources/android/icon/drawable-xhdpi-icon.png"
convert "$base" -resize '96x96'     -unsharp 1x4 "resources/android/icon/drawable-xhdpi-icon.png"
echo "resources/android/icon/drawable-xxhdpi-icon.png"
convert "$base" -resize '144x144'   -unsharp 1x4 "resources/android/icon/drawable-xxhdpi-icon.png"
echo "resources/android/icon/drawable-xxxhdpi-icon.png"
convert "$base" -resize '192x192'   -unsharp 1x4 "resources/android/icon/drawable-xxxhdpi-icon.png"


base="resources/splash.png"

# SPLASHES

# TODO iOS splashes

# Landscape
echo "resources/android/splash/drawable-land-ldpi-screen.png"
convert "$base" -resize '320x'  -gravity center -crop 320x200+0+0   -unsharp 1x4 "resources/android/splash/drawable-land-ldpi-screen.png"
echo "resources/android/splash/drawable-land-mdpi-screen.png"
convert "$base" -resize '480x'  -gravity center -crop 480x320+0+0   -unsharp 1x4 "resources/android/splash/drawable-land-mdpi-screen.png"
echo "resources/android/splash/drawable-land-hdpi-screen.png"
convert "$base" -resize '800x'  -gravity center -crop 800x480+0+0   -unsharp 1x4 "resources/android/splash/drawable-land-hdpi-screen.png"
echo "resources/android/splash/drawable-land-xhdpi-screen.png"
convert "$base" -resize '1280x' -gravity center -crop 1280x720+0+0  -unsharp 1x4 "resources/android/splash/drawable-land-xhdpi-screen.png"
echo "resources/android/splash/drawable-land-xxhdpi-screen.png"
convert "$base" -resize '1600x' -gravity center -crop 1600x960+0+0  -unsharp 1x4 "resources/android/splash/drawable-land-xxhdpi-screen.png"
echo "resources/android/splash/drawable-land-xxxhdpi-screen.png"
convert "$base" -resize '1920x' -gravity center -crop 1920x1280+0+0 -unsharp 1x4 "resources/android/splash/drawable-land-xxxhdpi-screen.png"

# Portrait
echo "resources/android/splash/drawable-port-ldpi-screen.png"
convert "$base" -resize 'x320'  -gravity center -crop 200x320+0+0   -unsharp 1x4 "resources/android/splash/drawable-port-ldpi-screen.png"
echo "resources/android/splash/drawable-port-mdpi-screen.png"
convert "$base" -resize 'x480'  -gravity center -crop 320x480+0+0   -unsharp 1x4 "resources/android/splash/drawable-port-mdpi-screen.png"
echo "resources/android/splash/drawable-port-hdpi-screen.png"
convert "$base" -resize 'x800'  -gravity center -crop 480x800+0+0   -unsharp 1x4 "resources/android/splash/drawable-port-hdpi-screen.png"
echo "resources/android/splash/drawable-port-xhdpi-screen.png"
convert "$base" -resize 'x1280' -gravity center -crop 720x1280+0+0  -unsharp 1x4 "resources/android/splash/drawable-port-xhdpi-screen.png"
echo "resources/android/splash/drawable-port-xxhdpi-screen.png"
convert "$base" -resize 'x1600' -gravity center -crop 960x1600+0+0  -unsharp 1x4 "resources/android/splash/drawable-port-xxhdpi-screen.png"
echo "resources/android/splash/drawable-port-xxxhdpi-screen.png"
convert "$base" -resize 'x1920' -gravity center -crop 1280x1920+0+0 -unsharp 1x4 "resources/android/splash/drawable-port-xxxhdpi-screen.png"
