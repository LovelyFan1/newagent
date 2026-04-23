# PHASE2_DEBUG_REPORT

## 企业抽样验证
- [三领域全覆盖] 比亚迪: PASS | expected=(True, True, True) got=(True, True, True)
- [三领域全覆盖] 力帆科技: PASS | expected=(True, True, True) got=(True, True, True)
- [三领域全覆盖] 中汽股份: PASS | expected=(True, True, True) got=(True, True, True)
- [财务+销售] 长城汽车: PASS | expected=(True, True, False) got=(True, True, False)
- [财务+销售] 长安汽车: PASS | expected=(True, True, False) got=(True, True, False)
- [财务+司法] 东风科技: PASS | expected=(True, False, True) got=(True, False, True)
- [销售+司法] 福田汽车: PASS | expected=(False, True, True) got=(False, True, True)
- [仅财务] 富奥股份: PASS | expected=(True, False, False) got=(True, False, False)
- [仅销售] 海马汽车: PASS | expected=(False, True, False) got=(False, True, False)
- [仅司法] 特力A: PASS | expected=(False, False, True) got=(False, False, True)

## 衍生指标计算验证（比亚迪 2022）
- current_ratio: 0.7224 -> PASS
- quick_ratio: 0.4851 -> PASS
- roe: 14.9712% -> PASS
- debt_asset_ratio: 0.7542 -> PASS

## 缺失字段处理验证
- 长城汽车 lawsuit_count=N/A, lawsuit_total_amount=N/A -> PASS
- 理想汽车不在当前企业基准库，改用仅销售样本 `海马汽车` 验证 -> INFO

## 边界情况测试
- 不存在企业: PASS | ValueError: stock_code=不存在的企业 year=2022 not found
- 无数据年份1999: PASS | status={'financial': False, 'sales': False, 'legal': False}

## 自动修复记录
- 修复：indicator_calc 财务缺失值不再默认写入 0，改为 None/N/A。
- 修复：新增 debt_asset_ratio（资产负债率）输出字段。
- 修复：法律维度缺失时保持 N/A，不再输出 0。

## 失败项汇总
- 无