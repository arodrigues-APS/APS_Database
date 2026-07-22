from aps.ml.iv_damage_research_cli import parser


def test_attended_research_workflow_commands_are_exposed():
    command_parser = parser()
    commands = command_parser._subparsers._group_actions[0].choices
    assert {
        "audit-cohort",
        "freeze-snapshot",
        "train-scalar",
        "train-hybrid",
        "score-historical",
        "status",
    } <= set(commands)


def test_mutating_commands_require_versions_and_actor():
    args = parser().parse_args(
        [
            "train-scalar",
            "--snapshot-version",
            "snapshot-v1",
            "--run-prefix",
            "research-v1",
            "--actor",
            "operator",
        ]
    )
    assert args.snapshot_version == "snapshot-v1"
    assert args.actor == "operator"
    assert args.seed == 17
