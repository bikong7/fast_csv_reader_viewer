QT       += core gui widgets concurrent

CONFIG += c++17

TARGET = bigcsvviewer
TEMPLATE = app

SOURCES += \
    csvloader.cpp \
    csvmodel.cpp \
    main.cpp \
    mainwindow.cpp
HEADERS += \
    csvloader.h \
    csvmodel.h \
    mainwindow.h

# Include paths if needed
#INCLUDEPATH +=

# Windows: define UTF-8 source
QMAKE_CXXFLAGS += -fexec-charset=UTF-8
