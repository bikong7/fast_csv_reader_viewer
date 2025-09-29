#include <QApplication>
#include <QMetaType>
#include <memory>

#include "csvloader.h"
#include "mainwindow.h"

int main(int argc, char *argv[]) {
  QApplication a(argc, argv);

  // register the shared_ptr<ParsedResult> type for signal/slot transfer
  qRegisterMetaType<std::shared_ptr<ParsedResult>>(
      "std::shared_ptr<ParsedResult>");

  MainWindow w;
  w.show();
  return a.exec();
}
