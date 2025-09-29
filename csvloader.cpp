#include "csvloader.h"

#include <QCoreApplication>
#include <QDebug>
#include <QFile>
#include <QFileInfo>
#include <QMetaType>
#include <cstdlib>
#include <cstring>

#include "fast_float/fast_float.h"  // 注意路径

// CsvLoader 构造函数，初始化文件路径和线程指针
CsvLoader::CsvLoader(const QString &filePath, QObject *parent)
    : QObject(parent), m_filePath(filePath), m_thread(nullptr) {}

// CsvLoader 析构函数，安全退出并释放线程
CsvLoader::~CsvLoader() {
  if (m_thread) {
    m_thread->quit();
    m_thread->wait();
    delete m_thread;
  }
}

// 启动加载流程，将当前对象移动到新线程并启动
void CsvLoader::start() {
  // 将此 worker 移动到专用线程
  m_thread = new QThread;
  this->moveToThread(m_thread);
  connect(m_thread, &QThread::started, this, &CsvLoader::process);
  // 结束后会在 process 内部 deleteLater
  m_thread->start();
}

// 判断字符是否为换行符
static inline bool isNewline(char c) { return c == '\n' || c == '\r'; }

// 处理 CSV 文件加载和解析的主流程
void CsvLoader::process() {
  // 打开文件
  QFile file(m_filePath);
  if (!file.open(QIODevice::ReadOnly)) {
    emit failed(tr("无法打开文件: %1").arg(m_filePath));
    if (m_thread) m_thread->quit();
    return;
  }

  // 获取文件大小
  qint64 fileSize = file.size();
  if (fileSize == 0) {
    emit failed(tr("文件为空"));
    if (m_thread) m_thread->quit();
    return;
  }

  // 内存映射文件内容
  uchar *mapPtr = file.map(0, fileSize);
  if (!mapPtr) {
    emit failed(tr("内存映射失败，文件太大或系统不支持"));
    if (m_thread) m_thread->quit();
    return;
  }

  const char *data = reinterpret_cast<const char *>(mapPtr);
  const char *end = data + fileSize;
  const char *cur = data;

  // 解析表头行
  const char *lineStart = cur;
  while (cur < end && *cur != '\n') ++cur;
  const char *lineEnd = cur;
  if (cur < end && *cur == '\n') ++cur;

  QString headerLine =
      QString::fromUtf8(lineStart, static_cast<int>(lineEnd - lineStart));
  QStringList headers = headerLine.split(',', QString::SkipEmptyParts);
  for (QString &h : headers) h = h.trimmed();
  size_t cols = static_cast<size_t>(headers.size());
  if (cols == 0) {
    emit failed(tr("无法解析表头或列数为0"));
    file.unmap(mapPtr);
    if (m_thread) m_thread->quit();
    return;
  }

  // 预分配数据存储空间
  auto vec = std::make_shared<std::vector<float>>();
  vec->reserve(1024 * 1024);  // 初始预分配

  size_t rows = 0;
  const char *p = cur;
  size_t colIndex = 0;
  const char *fieldStart = p;
  qint64 lastProgress = 0;

  // 主循环，逐字段解析数据
  while (p < end) {
    const char *f = p;
    while (f < end && *f != ',' && *f != '\n' && *f != '\r') ++f;

    // 使用 fast_float 解析字段
    if (f > p) {
      float value = 0.0f;
      auto result = fast_float::from_chars(p, f, value);
      if (result.ec != std::errc()) value = 0.0f;  // 解析失败则置 0
      vec->push_back(value);
    } else {
      vec->push_back(0.0f);  // 空字段
    }

    ++colIndex;

    // 指针前移
    if (f < end && *f == ',') {
      p = f + 1;
    } else {
      const char *q = f;
      while (q < end && (*q == '\r' || *q == '\n')) ++q;
      p = q;
      ++rows;
      colIndex = 0;
    }

    // 进度更新，每10%发一次信号
    qint64 processed = p - data;
    int percent = static_cast<int>((processed * 100) / fileSize);
    if (percent != lastProgress && (percent % 10 == 0)) {
      lastProgress = percent;
      emit progress(percent);
    }
  }

  // 最后一行可能没有换行符，补充行数
  if (colIndex != 0) ++rows;

  // 校正行数
  size_t totalFields = vec->size();
  if (rows * cols != totalFields && cols > 0) {
    rows = totalFields / cols;
  }

  vec->shrink_to_fit();

  // 构造解析结果结构体
  auto resultStruct = std::make_shared<ParsedResult>();
  resultStruct->headers = headers;
  resultStruct->rows = rows;
  resultStruct->cols = cols;
  resultStruct->data = vec;

  emit progress(100);
  emit finished(resultStruct);

  // 释放内存映射
  file.unmap(mapPtr);
  if (m_thread) m_thread->quit();
}
