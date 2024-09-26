//! Donburako
//!
//! データ駆動のワークフローを作成するためのライブラリ。
//! ワークフローは、ノードとエッジから構成される有向グラフで表現される。
//! ノードは、ワークフローのステップを表し、エッジはノード間の依存関係を表す。
//! 依存関係のないノードは並列に実行されるため、ワークフローの実行は効率的に行われる。
//!
//! # 概要
//!
//! ## コンポーネント
//!
//! ### ノード
//!
//! 実行されるタスクを表す。
//! 非同期に実行されるため、 IO や通信などの、 CPU バウンドでない処理を効率的に並列実行できる。
//! CPU バウンドかどうかを判断でき、バウンドしている場合は、ブロッキングスレッドを生成して並列実行する。そうでない場合は、非同期タスクとして実行する。
//! 実行可能になると、ノードはキューに追加され、最大の並列実行数を超えないように実行される。
//! 全てのノードのハンドルを `select!` マクロで一定時間ごとにチェックすることで、ノードの実行状況を監視できる。
//! ノードと外部のデータのやり取りは、ノード内部で行う。
//! ノード内部で入力を受け取るまで待機し、全ての入力を受け取ったら、処理を開始する。
//! 処理が完了したら全ての出力を返す。レジストリには何も入力されず、空になる。
//!
//! ### エッジ
//!
//! ノード間の依存関係を表す。
//! データ一つにつき、一つのエッジが存在する。そのデータがどこから来るか、どこに行くかを表す。
//! 一つのノードが複数のエッジを前にも後にも持つことができる。これにより、複数の引数と複数の戻り値を扱うことができる。
//! それぞれ始点と終点が同じかつ、データ型も同じようなエッジを許容する。
//!
//! ### ワークフロー
//!
//! ノードとエッジから構成される有向グラフで表現される。
//! ノードは、ワークフローのステップとして全て保存され、未完了のノードが存在する限り、ワークフローは実行され続ける。
//! 自身が依存しているノードが全て完了している場合、初めて実行可能になる。保存しているインデックスによって、メモリと計算量がトレードオフになる。
//! ノードの実行はプロセッサーによって行われる。
//!
//! ### レジストリ
//!
//! ワークフローの実行中にデータを保存するためのストレージ。
//! ワークフローの実行ごとに新しいレジストリが生成され、同一ワークフローを複数同時実行することができる。
//! レジストリごとに、処理が行われるため、ワークフロー間でのデータのやりとりは発生しない。データのやり取りをするのであれば、同一ワークフローにする必要がある。
//! レジストリの生成数が多い場合、メモリの使用量が増えるため、数を制限するのが望ましい。
//!
//! ### プロセッサー
//!
//! ノードを実行するためのプロセッサー。
//! ノードの実行数を制限し、並列実行を効率的に行う。
//! 複数のワークフローを同時に実行できるように、実行中のワークフローを管理する。
//! 実行するノードをワークフローから取得し、種類に応じて非同期スレッドかブロッキングスレッドで実行する。
//! 実行が完了したら、エッジにデータを紐付け、次のノードを実行する。
//! ワークフローの実行は、非同期タスクとして実行されるため、ワークフローの実行中に別のワークフローの実行を開始できる。
//!
//! ## ワークフローの実行
//!
//! ワークフローの実行は、以下の手順で行われる。
//!
//! 1. ワークフローを生成する。
//! 2. ワークフローをプロセッサーに登録する。
//! 3. レジストリを生成する
//! 4. レジストリに入力エッジとデータを登録する。
//! 5. レジストリを用いて、ワークフロー内で実行可能なノードを取得する。
//! 6. プロセッサーがノードを実行する。
//! 7. ノードの実行が完了したら、レジストリにエッジとデータを登録する。
//! 8. 5 ~ 7 を繰り返す。
//! 9. ワークフローが終了したら、レジストリからデータを取り出す。
//!
//! # 今後の展望
//!
//! ロギング、エラーハンドリング、リトライ、タイムアウトなど、ワークフローの実行に必要な機能を追加する。
//! ループと条件分岐をサポートする。

#![warn(missing_docs, rustdoc::missing_crate_level_docs)]

mod edge;
mod graph;
pub mod node;
mod registry;
pub mod workflow;
