# Werewolf: Slurm Parallel Pipeline and Analysis Guide

本 README 按你现在的实际环境来写：

- 代码目录在 `slurm-submit-00.cs.wisc.edu:/workspace/chen2744/Werewolf`
- 原始压缩包在 `/workspace/chen2744/Werewolf/data.tar`
- 所有代码都只在 Slurm 机器上运行
- 目标是先把并行 pipeline 搭好，再做描述统计、投票分析、公开发言分析、角色夜间行动分析，最后做一个简洁但讲故事能力强的模型部分

## 1. 项目结构

```text
Werewolf/
├─ analysis/
│  ├─ common.py
│  ├─ descriptive_analysis.py
│  ├─ vote_analysis.py
│  ├─ speech_analysis.py
│  ├─ role_analysis.py
│  └─ modeling.py
├─ scripts/
│  ├─ make_chunks.py
│  ├─ process_chunk.py
│  ├─ merge_outputs.py
│  └─ run_all_analyses.py
├─ slurm/
│  ├─ 00_create_env.sbatch
│  ├─ 01_extract_data.sbatch
│  ├─ 02_make_chunks.sbatch
│  ├─ 03_process_chunks_array.sbatch
│  ├─ 04_merge_outputs.sbatch
│  └─ 05_run_analyses.sbatch
├─ src/werewolf/
│  ├─ config.py
│  ├─ io_utils.py
│  └─ parse.py
├─ requirements.txt
└─ README.md
```

## 2. 这套并行方案为什么这样设计

你这个数据非常适合按文件级别并行，不需要先把单个大文件二次切块。

原因：

1. `data.tar` 里面本来就是大量独立 JSON 文件。
2. 每个 JSON 基本对应一局游戏，天然彼此独立。
3. 后续的大多数分析都只需要先把每局提成标准化表，再 merge。
4. 数据里存在不少 `0` 字节文件，所以必须在解析阶段就做错误记录和跳过。

因此这里采用的并行策略是：

1. 先解压 `data.tar`
2. 扫描 `data/*.json`
3. 用 `scripts/make_chunks.py` 按文件数切成 chunk manifest
4. 每个 Slurm array task 处理一个 chunk
5. 每个 chunk 输出自己的 parquet 表
6. 最后统一 merge 成全量 parquet
7. 后续所有分析脚本都只读 `processed/merged/*.parquet`

这比“先把每个 JSON 再拆成更细小块”更简单，也更稳。

## 3. 解析后会得到哪些标准表

`src/werewolf/parse.py` 会把每个原始 JSON 解析成下面这些表。

### `games`

每局一行，包含：

- `episode_id`
- `winner_team`
- `last_day`
- `game_end_reason`
- `public_discussion_count`
- `day_vote_count`
- `night_vote_count`
- `day_vote_no_exile_count`
- `terminated_with_agent_error`

### `players`

每局每位玩家一行，包含：

- `episode_id`
- `player_id`
- `role`
- `team`
- `model_name`
- `won`
- `survived_to_end`
- `alive_final`
- `eliminated_during_day`
- `eliminated_during_phase`
- `survival_days`

### `votes`

每次投票一行，白天和夜晚都保留，但做分析时会分别筛选。

- `phase == Day` 是白天放逐投票
- `phase == Night` 且 `actor_role == Werewolf` 是狼人夜间投票

包含：

- `actor_id`, `actor_role`
- `target_id`, `target_role`
- `is_day_vote`, `is_night_vote`
- `vote_target_is_werewolf`
- `day_exiled_player_id`
- `voted_for_exiled_player`
- `day_no_exile`
- `night_pack_target_id`
- `voted_for_pack_target`

### `speeches`

只保留公开发言，也就是 `discussion` 且 `public == True`。

包含：

- `actor_id`, `actor_role`
- `message`
- `message_chars`
- `message_words`
- `question_count`
- `mentioned_players_count`
- `won`

### `night_actions`

把夜间可分析的行动整理成统一表：

- `seer_inspect`
- `doctor_heal`
- `werewolf_vote`

包含：

- `actor_id`, `actor_role`
- `target_id`, `target_role`
- `target_is_werewolf`
- `target_is_power_role`
- `night_pack_target_id`
- `night_eliminated_player_id`
- `healed_pack_target`
- `successful_save`
- `voted_for_pack_target`

### `events`

轻量级全事件表，便于补查：

- `event_name`
- `day`, `phase`
- `public`
- `actor_id`, `target_id`
- `message`
- `description_short`

### `errors`

记录坏文件和空文件：

- `empty_file`
- `JSONDecodeError`
- 其他解析异常

## 4. 在 Slurm 机器上的完整操作流程

下面假设你已经 SSH 到 `slurm-submit-00.cs.wisc.edu`。

### 4.1 进入项目目录

```bash
cd /workspace/chen2744/Werewolf
export PROJECT_ROOT=/workspace/chen2744/Werewolf
```

### 4.2 可选：确认文件在不在

```bash
ls -lh
ls -lh "$PROJECT_ROOT/data.tar"
```

### 4.3 第一步：创建 Python 环境

```bash
sbatch slurm/00_create_env.sbatch
```

查看状态：

```bash
squeue -u $USER
```

看日志：

```bash
ls logs/slurm
tail -n 50 logs/slurm/ww-env-<jobid>.out
```

### 4.4 第二步：解压数据

这个作业会：

1. 创建 `logs/`, `processed/`, `outputs/`, `chunks/`
2. 如果 `data/` 已经存在并且有 JSON，就跳过
3. 否则从 `data.tar` 解压

```bash
sbatch slurm/01_extract_data.sbatch
```

完成后可以检查：

```bash
find data -maxdepth 1 -name '*.json' | wc -l
```

### 4.5 第三步：生成 chunk manifest

```bash
sbatch slurm/02_make_chunks.sbatch
```

它默认每个 chunk 放 `200` 个 JSON 文件。

为什么先用 `200`：

1. 单个文件大多在几 MB 到十几 MB
2. 200 个文件通常仍然适合作为一个 array task 的输入规模
3. 每个 task 的内存压力和输出大小比较可控
4. 对 Slurm 调度也更友好，不会一次性制造过多超小任务

完成后检查：

```bash
cat chunks/chunk_count.txt
head -n 5 chunks/chunk_00000.txt
```

### 4.6 第四步：提交 Slurm array 做并行解析

先读 chunk 数：

```bash
CHUNK_COUNT=$(cat "$PROJECT_ROOT/chunks/chunk_count.txt")
echo "$CHUNK_COUNT"
```

然后提交 array job。推荐先限制同时并发数，比如 `%24`。

```bash
sbatch --array=0-$((CHUNK_COUNT - 1))%24 slurm/03_process_chunks_array.sbatch
```

说明：

1. `0-$((CHUNK_COUNT - 1))` 表示每个 chunk 一个 task
2. `%24` 表示最多同时跑 24 个 task
3. 如果你的账号配额更紧，可以改成 `%12`
4. 如果你发现队列很空、资源充足，也可以改成 `%32`

处理中每个 task 会输出到：

```text
processed/chunks/chunk_00000/
processed/chunks/chunk_00001/
...
```

每个 chunk 目录里会有：

- `games.parquet`
- `players.parquet`
- `votes.parquet`
- `speeches.parquet`
- `night_actions.parquet`
- `events.parquet`
- `errors.parquet`
- `chunk_summary.csv`
- `metadata.json`

### 4.7 第五步：merge 所有 chunk 输出

等 array job 全部结束后：

```bash
sbatch slurm/04_merge_outputs.sbatch
```

完成后：

```bash
ls processed/merged
cat processed/merged/merge_summary.csv
```

### 4.8 第六步：跑所有分析和出图

```bash
sbatch slurm/05_run_analyses.sbatch
```

输出会写到：

```text
outputs/
├─ 01_descriptive/
├─ 02_vote_analysis/
├─ 03_speech_analysis/
├─ 04_role_analysis/
└─ 05_modeling/
```

## 5. 如果你想一步一步带依赖地提交

下面是更推荐的手工提交流程，因为你可以随时停下来检查结果。

### 5.1 创建环境

```bash
ENV_JOB=$(sbatch --parsable slurm/00_create_env.sbatch)
echo "$ENV_JOB"
```

### 5.2 解压数据，依赖环境完成

```bash
EXTRACT_JOB=$(sbatch --parsable --dependency=afterok:$ENV_JOB slurm/01_extract_data.sbatch)
echo "$EXTRACT_JOB"
```

### 5.3 生成 chunks，依赖解压完成

```bash
CHUNK_JOB=$(sbatch --parsable --dependency=afterok:$EXTRACT_JOB slurm/02_make_chunks.sbatch)
echo "$CHUNK_JOB"
```

等 `CHUNK_JOB` 结束后，手动读 `chunk_count.txt`，再提交 array。

### 5.4 提交 array

```bash
CHUNK_COUNT=$(cat "$PROJECT_ROOT/chunks/chunk_count.txt")
ARRAY_JOB=$(sbatch --parsable --array=0-$((CHUNK_COUNT - 1))%24 slurm/03_process_chunks_array.sbatch)
echo "$ARRAY_JOB"
```

### 5.5 merge

```bash
MERGE_JOB=$(sbatch --parsable --dependency=afterok:$ARRAY_JOB slurm/04_merge_outputs.sbatch)
echo "$MERGE_JOB"
```

### 5.6 analysis

```bash
ANALYSIS_JOB=$(sbatch --parsable --dependency=afterok:$MERGE_JOB slurm/05_run_analyses.sbatch)
echo "$ANALYSIS_JOB"
```

## 6. 如何单独重跑某一步

这是你后面最常用的。

### 只重跑 chunk 17

```bash
source .venv/bin/activate
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

python scripts/process_chunk.py \
  --chunk-file chunks/chunk_00017.txt \
  --output-root processed/chunks \
  --write-format parquet
```

### 只重新 merge

```bash
source .venv/bin/activate
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

python scripts/merge_outputs.py \
  --chunks-root processed/chunks \
  --merged-root processed/merged \
  --write-format parquet
```

### 只重跑某个分析脚本

例如只重跑投票分析：

```bash
source .venv/bin/activate
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

python -m analysis.vote_analysis \
  --processed-root processed/merged \
  --output-root outputs
```

同理：

```bash
python -m analysis.descriptive_analysis --processed-root processed/merged --output-root outputs
python -m analysis.speech_analysis --processed-root processed/merged --output-root outputs
python -m analysis.role_analysis --processed-root processed/merged --output-root outputs
python -m analysis.modeling --processed-root processed/merged --output-root outputs
```

## 7. 后续分析应该怎么做

下面这部分是最关键的分析路线图，已经和代码文件一一对应。

---

## 8. 第一部分：描述统计

对应代码：

- `analysis/descriptive_analysis.py`

### 目标

先把数据整体讲清楚，让老师一眼知道：

1. 数据有多大
2. 游戏是什么结构
3. 有多少局、多少玩家、多少公开发言、多少投票
4. 不同角色的基本结局有什么差异

### 这个脚本已经做了什么

输出：

- `dataset_overview.csv`
- `role_summary.csv`
- `winner_team_summary.csv`
- `overview.md`

图：

- `role_win_rate.png`
- `game_length_distribution.png`
- `winning_team_counts.png`
- `public_messages_by_role.png`

### 你在汇报里该怎么讲

建议先说这几句：

1. 数据由大量独立游戏日志组成，总规模足够大，适合用 Slurm 并行做文件级处理。
2. 每局固定为 8 位玩家，角色配置是 4 Villagers, 1 Doctor, 1 Seer, 2 Werewolves。
3. 我们先把原始日志标准化成玩家级、投票级、发言级、夜间行动级表，再进行统计建模。
4. 这样后续每个问题都可以在统一数据层上完成，具有可复现性。

### 你可以继续加的东西

如果时间够，可以额外加：

- `role` 与 `survived_to_end` 的列联表
- `role` 与 `won` 的卡方检验
- 游戏长度和赢家阵营关系

---

## 9. 第二部分：投票行为分析

对应代码：

- `analysis/vote_analysis.py`

### 研究问题

你可以把这一部分明确成：

1. 玩家白天投票是否投中狼人，和最终是否获胜有关吗？
2. Day 1 的投票是否已经提供了强信号？
3. 哪些角色更容易把票投到狼人身上？
4. 被投票较多的玩家，最后输赢有什么特征？

### 这个脚本已经提取的核心特征

玩家级特征在 `analysis/common.py` 里统一生成：

- `day_votes_cast`
- `day_votes_received`
- `day_votes_against_wolf`
- `vote_accuracy`
- `vote_majority_alignment`
- `day1_voted_wolf`
- `day1_voted_exiled`
- `day1_target_role`

### 已经生成的表和图

表：

- `vote_summary_by_role.csv`
- `win_rate_by_day1_voted_wolf.csv`
- `day1_vote_target_heatmap.csv`

图：

- `day1_vote_target_heatmap.png`
- `win_rate_by_day1_vote_correctness.png`
- `day_votes_received_by_outcome.png`
- `mean_vote_accuracy_by_role.png`

### 你该怎么解释这些图

推荐讲法：

1. `win_rate_by_day1_vote_correctness.png`
   如果 Day 1 就投中狼人时胜率明显更高，这会是一个非常强的叙事点。

2. `day1_vote_target_heatmap.png`
   看不同角色 Day 1 更倾向票给谁，尤其是 Villager / Seer / Werewolf 的差异。

3. `day_votes_received_by_outcome.png`
   如果经常被票的人更容易输，说明“被集体怀疑”本身是结果相关变量。

4. `mean_vote_accuracy_by_role.png`
   用来比较角色差异，但解释时要强调角色本身知道的信息不同，不能简单理解为“更聪明”。

### 如果你想进一步加强这一部分

建议额外写两个扩展：

1. 看 `Day 1` 和 `all days` 分开
2. 分角色看

最值得加的是：

- 只看 Villagers 阵营内部，投票投中狼人是否更能预测村民获胜
- 只看 Werewolves，是否更倾向把票投给 power role

---

## 10. 第三部分：公开发言分析

对应代码：

- `analysis/speech_analysis.py`

### 研究问题

你在 proposal 里说“不做 NLP，只分析次数/长度/简单特征”，这个方向是对的。

建议明确成：

1. 发言更多的人是否更容易赢？
2. 发言更长的人是否更容易赢？
3. 不同角色的公开发言风格是否不同？
4. Day 1 的发言强度是否和后续结果有关？

### 这个脚本用到的简单特征

- `public_message_count`
- `public_word_count`
- `avg_words_per_message`
- `question_count`
- `question_rate`
- `mentioned_players_total`
- `day1_public_message_count`
- `day1_public_word_count`

### 已经生成的表和图

表：

- `speech_summary_by_role.csv`
- `win_rate_by_message_bin.csv`

图：

- `public_message_count_by_role.png`
- `avg_words_per_message_by_role.png`
- `win_rate_by_message_volume.png`
- `speech_volume_scatter.png`

### 建议重点讲哪些

首选：

1. `win_rate_by_message_volume.png`
2. `public_message_count_by_role.png`
3. `avg_words_per_message_by_role.png`

因为这三张图最容易讲清楚：

- “沉默”和输赢关系
- “角色定位”和发言风格关系
- “发言长度”与表现关系

### 这里最容易犯的错误

不要直接说：

- “话多的人更强”
- “话少的人就是狼人”

更严谨的说法应该是：

- “在这个模拟数据里，公开发言强度与最终结果存在相关关系”
- “这种相关关系可能同时受角色信息量、存活时长和白天讨论位置影响”

---

## 11. 第四部分：角色夜间行动分析

对应代码：

- `analysis/role_analysis.py`

### 研究问题

这一部分建议拆成三个小问题：

1. Seer 有没有验到狼人，和村民最终获胜关系多大？
2. Doctor 有没有成功救到人，和村民最终获胜关系多大？
3. Werewolf 有没有优先打 power role，和狼人最终获胜关系多大？

### 已经抽好的角色特征

Seer：

- `seer_inspects`
- `seer_unique_targets`
- `seer_hit_wolf_count`
- `seer_hit_rate`

Doctor：

- `doctor_heals`
- `doctor_unique_heal_targets`
- `doctor_healed_pack_target_count`
- `doctor_successful_save_count`
- `doctor_save_rate`

Werewolf：

- `werewolf_night_votes`
- `werewolf_unique_targets`
- `werewolf_consensus_votes`
- `werewolf_consensus_rate`
- `werewolf_targeted_power_role_count`

### 已经生成的表和图

表：

- `seer_summary.csv`
- `doctor_summary.csv`
- `werewolf_summary.csv`

图：

- `seer_found_wolf_vs_win_rate.png`
- `doctor_save_vs_win_rate.png`
- `werewolf_target_power_role_vs_win_rate.png`

### 讲法建议

这一部分很适合做“机制解释”。

例如：

1. 如果 Seer 成功验出狼后，村民胜率明显上升，这说明信息获取在该博弈中价值很高。
2. 如果 Doctor 成功救人和村民胜率有关，说明保护关键角色很重要。
3. 如果 Werewolf 优先打 power role 时狼人胜率更高，说明针对信息角色和保护角色是高收益策略。

### 注意

Doctor 的 `successful_save` 是根据：

1. 医生保护目标
2. 狼人夜间选定目标
3. 夜间实际淘汰目标

三者关系推出来的。

如果你后面发现数据规则里还有更复杂的夜间机制，可以再补这个定义，但现在这个版本已经足够做项目分析。

---

## 12. 第五部分：最终讲故事和模型

对应代码：

- `analysis/modeling.py`

### 现在这份代码已经做了什么

做了两个轻量模型：

1. `LogisticRegression`
2. `DecisionTreeClassifier`

预测目标是：

- `won`

使用的输入特征包括：

- 存活时长
- 投票准确率
- 被投票次数
- 发言次数和长度
- 问句比例
- Seer 命中率
- Doctor 救人率
- Werewolf 共识率
- `role`

### 产出文件

表：

- `model_metrics.csv`
- `logistic_coefficients.csv`
- `decision_tree_importance.csv`

图：

- `logistic_coefficients.png`
- `decision_tree_importance.png`
- `decision_tree_structure.png`

### 你到底要不要放 logistic regression / decision tree

我的建议：

1. 要放
2. 但只放轻量版
3. 不要把它变成整份汇报的主角

最好的位置是最后一部分：

- 前面先用描述统计和图把故事讲清楚
- 最后说“我们还用一个简单模型检查这些特征是否有联合预测力”

### 最推荐的汇报表述

你可以这样说：

1. 我们先从单变量和分组图里看到投票、公开发言、角色行动与输赢存在明显关系。
2. 然后用 logistic regression 和一个浅层 decision tree 做联合检验。
3. 这些模型不是为了追求最强预测，而是为了帮助总结哪些特征在整体上最有解释力。

这样会比“我们训练了一个分类器”更成熟。

## 13. 汇报和 report 的推荐结构

### 5-7 分钟 presentation 建议

1. `Problem + Data`
   讲数据来源、规模、为什么需要 Slurm 并行。

2. `Pipeline`
   讲文件级 chunk、Slurm array、merge、标准化表。

3. `Descriptive Overview`
   1-2 张总览图。

4. `Voting Behavior`
   1-2 张最强图。

5. `Public Speech`
   1-2 张最直观图。

6. `Night Actions by Role`
   1-2 张机制图。

7. `Simple Model + Takeaway`
   1 张系数图或 tree 图。

8. `Limitations`
   比如：
   - 相关不等于因果
   - 模型 agent 之间可能存在系统性差异
   - 有些变量受角色信息不对称影响

### 600-word report 建议

Introduction：

- 数据是什么
- 问题是什么
- 并行计算怎么做
- 结论是什么

Body：

- 数据清理与标准化
- Slurm 并行细节
- 投票、发言、角色分析主要结果
- 简单模型结果

Conclusion：

- 回答最初问题
- 说明局限和未来工作

## 14. 如果你要继续扩展代码，优先顺序是什么

按性价比排序，我建议你接下来这样做：

1. 先真的在 Slurm 上跑完整套 pipeline，检查 `errors.parquet` 和输出图。
2. 先看 `02_vote_analysis` 和 `03_speech_analysis` 的结果，通常这两块最容易出有说服力的图。
3. 再看 `04_role_analysis`，挑最清楚的 2-3 张。
4. 最后再决定模型部分到底保留 logistic、tree，还是只保留一个。

## 15. 我建议你现在最先执行的命令

按顺序直接跑：

```bash
cd /workspace/chen2744/Werewolf
export PROJECT_ROOT=/workspace/chen2744/Werewolf

sbatch slurm/00_create_env.sbatch
sbatch slurm/01_extract_data.sbatch
sbatch slurm/02_make_chunks.sbatch
```

等 `02_make_chunks.sbatch` 完成后：

```bash
CHUNK_COUNT=$(cat "$PROJECT_ROOT/chunks/chunk_count.txt")
sbatch --array=0-$((CHUNK_COUNT - 1))%24 slurm/03_process_chunks_array.sbatch
```

等 array 完成后：

```bash
sbatch slurm/04_merge_outputs.sbatch
sbatch slurm/05_run_analyses.sbatch
```

## 16. 当前代码里的重要假设

为了先把工程搭起来，这一版默认了下面这些假设：

1. 每个非空 JSON 对应一局游戏。
2. 公开发言只取 `discussion` 且 `public == True`。
3. 白天投票看 `vote_action` 且 `phase == Day`。
4. 夜间角色分析主要基于 `inspect_action`、`heal_action`、狼人 `vote_action`。
5. 游戏配置基本稳定，主要角色为 `Villager / Doctor / Seer / Werewolf`。

如果你后面发现某些日志还有别的变体事件名，再在 `src/werewolf/parse.py` 里补即可。

## 17. 建议你做的第一轮结果检查

当第一次全跑完后，优先看这些文件：

```text
processed/merged/merge_summary.csv
processed/merged/errors.parquet
outputs/01_descriptive/tables/dataset_overview.csv
outputs/02_vote_analysis/figures/win_rate_by_day1_vote_correctness.png
outputs/03_speech_analysis/figures/win_rate_by_message_volume.png
outputs/04_role_analysis/figures/seer_found_wolf_vs_win_rate.png
outputs/05_modeling/tables/model_metrics.csv
```

如果这些都正常，就说明你的主线已经打通了。
