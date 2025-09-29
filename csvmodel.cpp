#include "csvmodel.h"

#include <QBrush>
#include <QColor>
#include <QVariant>

// CsvModel 构造函数，初始化表头、行数、列数和数据指针
CsvModel::CsvModel(const QStringList &headers, size_t rows, size_t cols,
                   std::shared_ptr<std::vector<float>> data, QObject *parent)
    : QAbstractTableModel(parent),
      m_headers(headers),
      m_rows(rows),
      m_cols(cols),
      m_data(data) {}

// 返回行数
int CsvModel::rowCount(const QModelIndex &parent) const {
  Q_UNUSED(parent);
  return static_cast<int>(m_rows);
}

// 返回列数
int CsvModel::columnCount(const QModelIndex &parent) const {
  Q_UNUSED(parent);
  return static_cast<int>(m_cols);
}

// 返回指定单元格的数据
QVariant CsvModel::data(const QModelIndex &index, int role) const {
  if (!index.isValid()) return QVariant();

  if (role == Qt::DisplayRole) {
    size_t row = static_cast<size_t>(index.row());
    size_t col = static_cast<size_t>(index.column());
    size_t idx = row * m_cols + col;
    if (m_data && idx < m_data->size()) {
      return QVariant((*m_data)[idx]);
    }
  }
  return QVariant();
}

// 返回表头数据
QVariant CsvModel::headerData(int section, Qt::Orientation orientation,
                              int role) const {
  if (role != Qt::DisplayRole) return QVariant();

  if (orientation == Qt::Horizontal) {
    if (section >= 0 && section < m_headers.size()) {
      return m_headers[section];
    }
  } else if (orientation == Qt::Vertical) {
    return section + 1;
  }
}
