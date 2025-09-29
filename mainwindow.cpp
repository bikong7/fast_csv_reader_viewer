#include "mainwindow.h"

#include <QFileDialog>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QMessageBox>
#include <QProgressBar>
#include <QPushButton>
#include <QTableView>
#include <QVBoxLayout>
#include <QtConcurrent>

#include "csvloader.h"
#include "csvmodel.h"

// 主窗口构造函数，初始化界面和控件
MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent),
      m_btnSelect(new QPushButton(tr("选择 CSV 文件"), this)),
      m_progress(new QProgressBar(this)),
      m_tableView(new QTableView(this)),
      m_model(nullptr) {
  QWidget *central = new QWidget(this);
  setCentralWidget(central);
  setWindowTitle("Big CSV Viewer (memory-mapped)");

  m_progress->setRange(0, 100);
  m_progress->setValue(0);
  m_progress->setTextVisible(true);

  QHBoxLayout *topLayout = new QHBoxLayout;
  topLayout->addWidget(m_btnSelect);
  topLayout->addWidget(new QLabel(tr("导入进度："), this));
  topLayout->addWidget(m_progress);

  QVBoxLayout *mainLayout = new QVBoxLayout;
  mainLayout->addLayout(topLayout);
  mainLayout->addWidget(m_tableView);
  central->setLayout(mainLayout);

  connect(m_btnSelect, &QPushButton::clicked, this, &MainWindow::onSelectFile);

  resize(1200, 800);
}

// 主窗口析构函数，释放 model
MainWindow::~MainWindow() {
  delete m_model;  // QTableView 默认不负责释放 model
}

// 选择文件按钮点击事件，弹出文件选择对话框并启动加载流程
void MainWindow::onSelectFile() {
  QString file = QFileDialog::getOpenFileName(this, "选择 CSV 文件");
  if (file.isEmpty()) return;

  m_progress->setValue(0);
  m_btnSelect->setEnabled(false);

  // 创建 CsvLoader，不设置父对象
  CsvLoader *loader = new CsvLoader(file);

  // 创建线程，并设置父对象为当前窗口
  QThread *thread = new QThread(this);

  loader->moveToThread(thread);

  // 连接线程启动信号到 loader 的处理函数
  connect(thread, &QThread::started, loader, &CsvLoader::process);

  // 连接 loader 完成信号到主窗口的处理槽，并负责资源释放
  connect(loader, &CsvLoader::finished, this,
          [=](std::shared_ptr<ParsedResult> result) {
            onLoadFinished(result);
            loader->deleteLater();
            thread->quit();
          });

  // 连接 loader 失败信号到主窗口的处理槽，并负责资源释放
  connect(loader, &CsvLoader::failed, this, [=](const QString &err) {
    QMessageBox::critical(this, "导入失败", err);
    loader->deleteLater();
    thread->quit();
  });

  // 线程结束时自动释放线程对象
  connect(thread, &QThread::finished, thread, &QThread::deleteLater);

  // 连接进度信号到进度条
  connect(loader, &CsvLoader::progress, this, &MainWindow::onProgress);

  thread->start();
}

// 进度条更新槽函数
void MainWindow::onProgress(int percent) { m_progress->setValue(percent); }

// 加载失败时的处理槽函数
void MainWindow::onLoadFailed(const QString &err) {
  m_btnSelect->setEnabled(true);
  QMessageBox::critical(this, tr("导入失败"), err);
}

// 加载完成时的处理槽函数，创建并绑定 model，设置表格属性
void MainWindow::onLoadFinished(std::shared_ptr<ParsedResult> result) {
  m_btnSelect->setEnabled(true);
  m_progress->setValue(100);

  // 创建 model 并绑定到 TableView
  delete m_model;
  m_model = new CsvModel(result->headers, result->rows, result->cols,
                         result->data, this);
  m_tableView->setModel(m_model);

  // 针对大数据集的表格显示优化
  m_tableView->setAlternatingRowColors(false);
  m_tableView->verticalHeader()->setDefaultSectionSize(18);
  m_tableView->setSortingEnabled(false);
  m_tableView->setSelectionBehavior(QAbstractItemView::SelectRows);
  m_tableView->setSelectionMode(QAbstractItemView::SingleSelection);
}