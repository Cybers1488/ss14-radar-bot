#!/usr/bin/env python3
"""
Lust Station Prometheus Intel Bot — Multi-Server Real-Time Telemetry Radar
Monitors both SPECTRAL and VENUS (and any other configured server) in parallel.
Dispatches server-specific alerts into dedicated Discord channels (#spectrum, #venera).
"""

import json
import sys
import os
import asyncio
from pathlib import Path
import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
import re
from datetime import datetime, timezone

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

CONFIG_PATH = Path(__file__).parent / "bot_config.json"

PACING_MAP = {
    0: "Отдых (Relaxation)",
    1: "Нагнетание (BuildUp)",
    2: "Пик угроз (Peak)",
    3: "Восстановление (Recovery)"
}

ALERT_MAP = {
    0: "Зеленый (0)",
    1: "Синий (1)",
    2: "Желтый (2)",
    3: "Красный (3)",
    4: "Гамма / Дельта (4)"
}

PRESET_NAMES = {
    "Traitor": "Предатель (Traitor)",
    "TraitorReinforcements": "Предатели с подкреплением",
    "Nukeops": "Ядерные оперативники (Nukeops)",
    "Revolutionary": "Революция (Revolutionary)",
    "Zombie": "Зомби-инвазия (Zombie)",
    "Zombieteors": "Зомби-метеоры (Zombieteors)",
    "Changeling": "Генокрад (Changeling)",
    "Thief": "Воры (Thief)",
    "Vampire": "Вампиры (Vampire)",
    "SubvertedSilicon": "Взломанные киборги (Subverted)",
    "Wizard": "Маг (Wizard)",
    "SpaceNinja": "Космический ниндзя",
    "Greenshift": "Навозная Яма",
    "Extended": "Навозная Яма",
    "Survival": "Выживание (Survival)",
    "KesslerSyndrome": "Синдром Кесслера (KesslerSyndrome)",
    "AllAtOnce": "Все сразу (AllAtOnce)",
    "ExtraLowPop": "Низкий онлайн (ExtraLowPop)",
    "Dynamic": "Динамический режим (Dynamic)",
    "Secret": "Секрет (Secret)"
}

PRESET_ANTAG_NAMES = {
    "Traitor": "Предатели Синдиката (Traitor)",
    "TraitorReinforcements": "Предатели с подкреплением",
    "Nukeops": "Ядерные оперативники (Nukeops)",
    "Revolutionary": "Главы революции (Revolutionary)",
    "Zombie": "Зомби-инвазия (Zombie)",
    "Zombieteors": "Зомби-метеоры (Zombie)",
    "Changeling": "Генокрады (Changeling)",
    "Thief": "Воры (Thief)",
    "Vampire": "Вампиры (Vampire)",
    "SubvertedSilicon": "Взломанные киборги",
    "Wizard": "Маги (Wizard)",
    "SpaceNinja": "Космический ниндзя",
    "Greenshift": "Мидраунд антагонист",
    "Extended": "Мидраунд антагонист",
    "Навозная Яма": "Мидраунд антагонист",
    "Survival": "Выживание",
    "Dynamic": "Динамический режим"
}

PRESET_RULES = {
    "Traitor": [
        "Traitor",
        "DummyNonAntagChance",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "TraitorReinforcements": [
        "Traitor",
        "TraitorReinforcements",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Nukeops": [
        "Nukeops",
        "DummyNonAntagChance",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Revolutionary": [
        "Revolutionary",
        "DummyNonAntagChance",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Zombie": [
        "Zombie",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Zombieteors": [
        "Zombie",
        "BasicStationEventScheduler",
        "KesslerSyndromeScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Changeling": [
        "Changeling",
        "DummyNonAntagChance",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Thief": [
        "Thief",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Vampire": [
        "Vampire",
        "DummyNonAntagChance",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "SubvertedSilicon": [
        "SubvertedSilicon",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Wizard": [
        "Wizard",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "SpaceNinja": [
        "SpaceNinja",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Greenshift": [
        "DummyNonAntag",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Extended": [
        "DummyNonAntag",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Survival": [
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "KesslerSyndrome": [
        "KesslerSyndromeScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "AllAtOnce": [
        "Traitor",
        "Nukeops",
        "Revolutionary",
        "Zombie",
        "Changeling",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "ExtraLowPop": [
        "SubGamemodesRule",
        "BasicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "Dynamic": [
        "DynamicRule",
        "DummyNonAntag",
        "DynamicStationEventScheduler",
        "MeteorSwarmScheduler",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "StorytellerClassic": [
        "StorytellerClassicRule",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "StorytellerCalm": [
        "StorytellerCalmRule",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ],
    "StorytellerInsane": [
        "StorytellerInsaneRule",
        "SpaceTrafficControlEventScheduler",
        "BasicRoundstartVariation"
    ]
}

GHOST_ROLE_EVENTS = {
    "LoneOpsSpawn": "Одинокий ядерный оперативник (LoneOps)",
    "DragonSpawn": "Космический дракон (Dragon)",
    "NinjaSpawn": "Космический ниндзя (SpaceNinja)",
    "SpaceNinja": "Космический ниндзя (SpaceNinja)",
    "Wizard": "Маг (Wizard)",
    "WizardSpawn": "Маг (Wizard)",
    "AssaultOps": "Штурмовой отряд Синдиката (AssaultOps)",
    "AbductorsSpawn": "Похитители / Абдукторы (Abductors)",
    "DerelictSyndicateAssaultCyborgSpawn": "Штурмовой киборг Синдиката",
    "PirateSmall": "Пиратский малый шаттл",
    "PirateMedium": "Боевой корвет пиратов",
    "KingRatMigration": "Крысиный король (KingRat)",
    "RevenantSpawn": "Ревенант (Revenant)",
    "Fugitive": "Беглец (Fugitive)",
    "UnknownShuttleManOWar": "Экипаж фрегата Man-O-War",
    "ParadoxCloneSpawn": "Парадоксальный клон"
}

MIDROUND_ANTAG_EVENTS = {
    "SleeperAgents": "Спящие агенты Синдиката (активация)",
    "Thief": "Воры (мидраунд)",
    "Traitor": "Предатели (мидраунд)",
    "Zombie": "Зомби-инфекция (мидраунд)",
    "Nukeops": "Ядерные оперативники",
    "SiliconLiberation": "Освобождение кремния (восстание боргов)"
}

ANTAG_ROLE_NAMES = {
    "PiratesSpawn": "Пираты (Pirates)",
    "SpacePirates": "Космические пираты (SpacePirates)",
    "Pirates": "Пираты (Pirates)",
    "Pirate": "Пираты (Pirates)",
    "LoneAbductorSpawn": "Одинокий абдуктор (LoneAbductor)",
    "LoneAbductorAntag": "Одинокий абдуктор (LoneAbductor)",
    "DuoAbductorSpawn": "Абдукторы (DuoAbductors)",
    "AbductorVictim": "Подопытный абдукторов (AbductorVictim)",
    "AbductorsSpawn": "Абдукторы (Abductors)",
    "Abductor": "Абдуктор (Abductor)",
    "SlasherSpawn": "Слэшер (Мясник)",
    "Slasher": "Слэшер (Мясник)",
    "ContractorSpawn": "Контрактор (Contractor)",
    "ContractorSpawnMidround": "Контрактор (Contractor)",
    "Contractor": "Контрактор (Contractor)",
    "FloorGoblinSpawn": "Подпольный гоблин (FloorGoblin)",
    "SantaClausSpawn": "Санта Клаус (SantaClaus)",
    "MalignRiftSpawn": "Разлом скверны (MalignRift)",
    "ColossusSpawn": "Колосс (Colossus)",
    "BlobSpawn": "Блоб (Blob)",
    "DerelictCyborgSyndicateSpawn": "Заброшенный киборг Синдиката",
    "Traitor": "Предатель (Traitor)",
    "TraitorSleeper": "Спящий агент Синдиката (Sleeper Agent)",
    "Thief": "Вор (Thief)",
    "Changeling": "Генокрад (Changeling)",
    "Wizard": "Маг (Wizard)",
    "WizardSpawn": "Маг (WizardSpawn)",
    "Dragon": "Космический дракон (Dragon)",
    "DragonSpawn": "Космический дракон (DragonSpawn)",
    "SpaceNinja": "Космический ниндзя (SpaceNinja)",
    "NinjaSpawn": "Космический ниндзя (NinjaSpawn)",
    "LoneAbductorAntag": "Одинокий абдуктор (LoneAbductorAntag)",
    "AbductorsSpawn": "Похитители / Абдукторы (AbductorsSpawn)",
    "Fugitive": "Беглец (Fugitive)",
    "ParadoxClone": "Парадоксальный клон (ParadoxClone)",
    "ParadoxCloneSpawn": "Парадоксальный клон (ParadoxCloneSpawn)",
    "SELFAgent": "Спецагент Синдиката (SELF)",
    "GenericTeamAntagonist": "Оперативник Синдиката / Ядерщик",
    "Nukeops": "Ядерный оперативник (Nukeops)",
    "LoneOpsSpawn": "Одинокий ядерщик (LoneOpsSpawn)",
    "Slasher": "Слэшер (Мясник)",
    "SlasherSpawn": "Слэшер (Мясник)",
    "SlaughterDemon": "Слотердемон",
    "SlaughterDemonSpawn": "Слотердемон",
    "Hastur": "Хастур",
    "Wraith": "Рейф",
    "Zombie": "Зомби (Zombie)",
    "Heretic": "Еретик (Heretic)",
    "Revenant": "Ревенант (Revenant)",
    "RevenantSpawn": "Ревенант (RevenantSpawn)",
    "DerelictSyndicateAssaultCyborgSpawn": "Штурмовой киборг Синдиката (DerelictSyndicateAssaultCyborgSpawn)",
    "AssaultOps": "Штурмовой отряд Синдиката (AssaultOps)",
    "PirateSmall": "Пиратский малый шаттл (PirateSmall)",
    "PirateMedium": "Боевой корвет пиратов (PirateMedium)",
    "KingRatMigration": "Крысиный король (KingRatMigration)",
    "SubvertedSilicon": "Взломанный ИИ / киборг (SubvertedSilicon)",
    "HeadRev": "Глава революции (HeadRev)",
    "Revolutionary": "Революционер (Revolutionary)",
    "Blob": "Блоб (Blob)",
    "Shadowling": "Тенеморф (Shadowling)",
    "SiliconLiberation": "Восстание боргов (SiliconLiberation)",
    "SleeperAgents": "Спящие агенты (SleeperAgents)"
}

DISASTER_EVENTS = {
    "ClownGoblinMigration": "Миграция клоунов-гоблинов",
    "CockroachMigration": "Миграция тараканов",
    "SnailMigrationLowPop": "Миграция улиток",
    "SnailMigration": "Миграция улиток",
    "DropPodEngineeringRepair": "Инженерный дроппод ремонта",
    "DerelictGenericCyborgSpawn": "Заброшенный стандартный киборг",
    "DerelictEngineerCyborgSpawn": "Заброшенный инженерный киборг",
    "DerelictJanitorCyborgSpawn": "Заброшенный уборочный киборг",
    "DerelictMedicalCyborgSpawn": "Заброшенный медицинский киборг",
    "DerelictMiningCyborgSpawn": "Заброшенный шахтерский киборг",
    "LustRadiationStorm": "Радиационный шторм",
    "LustRadiationOutburst": "Радиационная вспышка",
    "SolarFlare": "Вспышка на солнце (отключение связи)",
    "IonStorm": "Ионный шторм (сбой законов ИИ / электроники)",
    "MassHallucinations": "Массовые галлюцинации экипажа",
    "MeteorSwarmLarge": "Крупный метеоритный поток",
    "MeteorSwarmMedium": "Средний метеоритный поток",
    "MeteorSwarmSmall": "Малый метеоритный поток",
    "SpaceDustMajor": "Мажорный космический мусор",
    "SpaceDustMinor": "Минорный космический мусор",
    "PowerGridCheck": "Сбой в электросети (перегрузка)",
    "BreakerFlip": "Отключение автоматов питания",
    "GasLeak": "Критическая утечка опасных газов",
    "KudzuGrowth": "Разрастание кудзу",
    "AnomalySpawn": "Спавн аномалии",
    "BluespaceArtifact": "Активация блюспейс-артефакта",
    "BluespaceLocker": "Аномальный блюспейс-шкаф",
    "VentFleshWorms": "Черви вентиляции",
    "VentbullMigration": "Прорыв стаи вентбуллов",
    "SpiderSpawnHorde": "Нашествие пауков",
    "SpiderClownSpawnHorde": "Нашествие пауков-клоунов",
    "SlimesSpawnHorde": "Вспышка размножения слизней",
    "SnakeSpawnHorde": "Нашествие змей",
    "RandomCritterHorde": "Нашествие агрессивной фауны",
    "RandomSentience": "Пробуждение разума у животных",
    "EmergencyGridRestore": "Аварийное восстановление энергосети",
    "ClericalError": "Канцелярская ошибка",
    "GameRuleMeteorSwarmSmall": "Малый метеоритный поток",
    "GameRuleSpaceDustMinor": "Малый космический мусор"
}


def format_rules_stack(preset: str) -> str:
    rules = PRESET_RULES.get(preset, [preset, "BasicStationEventScheduler", "BasicRoundstartVariation"])
    return "\n".join([format_rule_entry(r) for r in rules])


GOOB_PRESETS = {
    "TheGuide": {
        "title": "Проводник (The Guide / Combat Dynamic)",
        "rules": ["CombatDynamic", "SubGamemodesRule", "IonStormScheduler", "GameDirectorRulesTable"]
    },
    "TheGhost": {
        "title": "Призрак (The Ghost / Calm Dynamic)",
        "rules": ["CalmDynamic", "SubGamemodesRule", "RareIonStormScheduler", "GameDirectorRulesTable"]
    },
    "TheTempest": {
        "title": "Буря (The Tempest / Chaotic Dynamic)",
        "rules": ["ChaoticDynamic", "SubGamemodesRule", "IonStormScheduler", "GameDirectorRulesTable"]
    },
    "TheWatcher": {
        "title": "Наблюдатель (The Watcher / Classic Dynamic)",
        "rules": ["ClassicDynamic", "SubGamemodesRule", "IonStormScheduler", "GameDirectorRulesTable"]
    },
    "SecretPlusLow": {
        "title": "Секрет Плюс: Низкая угроза",
        "rules": ["SecretPlusLow", "MeteorSwarmMildScheduler"]
    },
    "SecretPlusMid": {
        "title": "Секрет Плюс: Средняя энтропия",
        "rules": ["SecretPlusMid", "MeteorSwarmScheduler"]
    },
    "SecretPlusHigh": {
        "title": "Секрет Плюс: Высокая угроза",
        "rules": ["SecretPlusHigh", "MeteorSwarmScheduler"]
    },
    "SecretPlusAdmeme": {
        "title": "Секрет Плюс: Адмемы",
        "rules": ["SecretPlusAdmeme", "MeteorSwarmScheduler"]
    },
    "SurvivalPlusMid": {
        "title": "Выживание Плюс",
        "rules": ["SecretPlusRampingMid", "MeteorSwarmScheduler"]
    },
    "Changeling": {
        "title": "Генокрад (Changeling)",
        "rules": ["Changeling", "SubGamemodesRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "BasicRoundstartVariation"]
    },
    "Traitorling": {
        "title": "Предатели и Генокрады (Traitorling)",
        "rules": ["CalmLing", "CalmTraitor", "BasicStationEventScheduler", "MeteorSwarmScheduler", "BasicRoundstartVariation"]
    },
    "NukeTraitor": {
        "title": "Ядерные оперативники и Предатели (NukeTraitor)",
        "rules": ["Calmops", "CalmTraitor", "BasicStationEventScheduler", "MeteorSwarmScheduler", "BasicRoundstartVariation"]
    },
    "NukeLing": {
        "title": "Ядерные оперативники и Генокрады (NukeLing)",
        "rules": ["Calmops", "CalmLing", "BasicStationEventScheduler", "MeteorSwarmScheduler", "BasicRoundstartVariation"]
    },
    "RevTraitor": {
        "title": "Революция и Предатели (RevTraitor)",
        "rules": ["CalmRevs", "CalmTraitor", "BasicStationEventScheduler", "MeteorSwarmScheduler"]
    },
    "RevLing": {
        "title": "Революция и Генокрады (RevLing)",
        "rules": ["CalmRevs", "CalmLing", "BasicStationEventScheduler", "MeteorSwarmScheduler", "BasicRoundstartVariation"]
    },
    "Blob": {
        "title": "Блоб (Blob)",
        "rules": ["BlobGameMode", "BasicStationEventScheduler", "AntagStationEventScheduler", "BasicRoundstartVariation", "SubGamemodesRule"]
    },
    "Honkops": {
        "title": "Хонкоперы (Honkops)",
        "rules": ["Honkops", "SubGamemodesRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation"]
    },
    "OopsAllThieves": {
        "title": "Одни Воры (Oops All Thieves)",
        "rules": ["OopsAllThieves", "TraitorSubGamemodesRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation"]
    },
    "SecretPlusLowpop": {
        "title": "Секрет Плюс: Лоупоп",
        "rules": ["SecretPlusLowpop", "MeteorSwarmMildScheduler"]
    }
}

GOOB_STORIES = {
    "Relaxed": "Спокойный (Relaxed)",
    "Science": "Научный (Science)",
    "RelaxedAttack": "Внезапная атака (RelaxedAttack)",
    "ScienceAttack": "Безумная наука и штурм (ScienceAttack)",
    "MajorCombat": "Масштабные боевые действия (MajorCombat)",
    "SlowBurn": "Медленное нагнетание (SlowBurn)",
    "Siege": "Осада станции (Siege)",
    "Infestation": "Нашествие / Заражение (Infestation)",
    "Blackout": "Полное обесточивание (Blackout)",
    "SkeletonCrew": "Минимальный экипаж (SkeletonCrew)",
    "QuietShift": "Тихая смена (QuietShift)"
}

GOOB_BEATS = {
    "Peace": "Мирная фаза (Peace)",
    "PowerIssues": "Проблемы с питанием (PowerIssues)",
    "Brownout": "Падение напряжения (Brownout)",
    "MadScience": "Опыты вышли из-под контроля (MadScience)",
    "Vermin": "Нашествие паразитов (Vermin)",
    "Attackers": "Штурм станции (Attackers)",
    "AttackMild": "Малая атака (AttackMild)",
    "RestoreOrder": "Восстановление порядка (RestoreOrder)",
    "RepairStation": "Ремонт станции (RepairStation)",
    "Denouement": "Развязка / Финал раунда (Denouement)"
}


def format_goob_rules_stack(
    preset_name: str,
    roundstart_antags: list = None,
    story: str = None,
    beat: str = None,
    rule_spawn_times: dict[str, str] = None,
    midround_rules: list = None
) -> str:
    preset_info = GOOB_PRESETS.get(preset_name, {})
    rules = list(preset_info.get("rules", [preset_name, "GameDirectorRulesTable"]))
    if roundstart_antags:
        for rs in roundstart_antags:
            mapped = "CalmTraitor" if rs == "Traitor" else ("Calmops" if rs == "Nukeops" else ("CalmLing" if rs == "Changeling" else f"AntagSelection:{rs}"))
            if mapped not in rules:
                rules.append(mapped)
    if midround_rules:
        for mr in midround_rules:
            if mr not in rules:
                rules.append(mr)
    if story:
        rules.append(f"Story:{story}")
    if beat:
        rules.append(f"StoryBeat:{beat}")
    rule_spawn_times = rule_spawn_times or {}
    return "\n".join([format_rule_entry(r, rule_spawn_times.get(r, "00:00")) for r in rules])


def get_goob_target(server_name: str) -> tuple[str, str, str]:
    s = server_name.lower()
    if "basil" in s or "omega" in s:
        return "https://omega.goobstation.com/status", "https://omega.goobstation.com/metrics", "GOOB-BASILEUS"
    return "https://sigma.goobstation.com/status", "https://sigma.goobstation.com/metrics", "GOOB-SIGMA"


def get_antag_display(rule: str) -> str:
    if rule in ANTAG_ROLE_NAMES:
        return ANTAG_ROLE_NAMES[rule]
    if rule in GHOST_ROLE_EVENTS:
        val = GHOST_ROLE_EVENTS[rule]
        return val if "(" in val else f"{val} ({rule})"
    if rule in MIDROUND_ANTAG_EVENTS:
        val = MIDROUND_ANTAG_EVENTS[rule]
        return val if "(" in val else f"{val} ({rule})"
    return rule


ROUNDSTART_ANTAG_RULES = {
    "Heretic", "CalmHeretic", "Thief", "OopsAllThieves",
    "Revolutionary", "CalmRevs", "HeadRev"
}


MAJOR_ANTAG_KEYWORDS = (
    "ninja", "butcher", "loneop", "lone_op", "nukeop", "nuke",
    "operative", "assaultops", "dragon", "wizard", "revenant",
    "blob", "heretic", "changeling", "ling", "abductor", "zombie",
    "pirate", "headrev", "shadowling", "slaughter", "hastur", "wraith",
    "demon", "slasher", "darklord", "vox", "contractor", "gang",
    "calmops", "calmling", "calmheretic"
)



LOTTERY_ANTAG_RULES = {
    "LoneOpsSpawn", "LoneOps", "SpacePirates", "PiratesSpawn", "Pirates", "Pirate",
    "LoneAbductorSpawn", "DuoAbductorSpawn", "AbductorsSpawn", "Abductor",
    "SlasherSpawn", "Slasher", "DragonSpawn", "Dragon",
    "NinjaSpawn", "SpaceNinja", "WizardSpawn", "Wizard",
    "BlobSpawn", "Blob", "RevenantSpawn", "Revenant",
    "FloorGoblinSpawn", "ColossusSpawn", "SantaClausSpawn",
    "MalignRiftSpawn", "ContractorSpawn", "ContractorSpawnMidround",
    "DerelictCyborgSyndicateSpawn", "DerelictSyndicateAssaultCyborgSpawn",
    "AssaultOps"
}

EVENT_TO_ANTAG_RULE = {
    "SpacePirates": "PiratesSpawn",
    "Pirates": "PiratesSpawn",
    "PiratesSpawn": "PiratesSpawn",
    "LoneOps": "LoneOpsSpawn",
    "LoneOpsSpawn": "LoneOpsSpawn",
    "NinjaSpawn": "NinjaSpawn",
    "SpaceNinja": "NinjaSpawn",
    "DragonSpawn": "DragonSpawn",
    "WizardSpawn": "WizardSpawn",
    "SlasherSpawn": "SlasherSpawn",
    "LoneAbductorSpawn": "LoneAbductorSpawn",
    "DuoAbductorSpawn": "DuoAbductorSpawn",
    "RevenantSpawn": "RevenantSpawn",
    "BlobSpawn": "BlobSpawn",
    "FloorGoblinSpawn": "FloorGoblinSpawn",
    "ColossusSpawn": "ColossusSpawn",
    "SantaClausSpawn": "SantaClausSpawn",
    "MalignRiftSpawn": "MalignRiftSpawn",
    "ContractorSpawn": "ContractorSpawn",
    "ContractorSpawnMidround": "ContractorSpawn",
    "DerelictCyborgSyndicateSpawn": "DerelictCyborgSyndicateSpawn"
}

def is_major_antag(rule: str) -> bool:
    if not rule:
        return False
    low = str(rule).lower()
    if "victim" in low:
        return False
    return any(k in low for k in MAJOR_ANTAG_KEYWORDS)


GAMERULE_DESCRIPTIONS = {
    # Core & Dynamic Schedulers
    "CombatDynamic": "Боевой динамик (The Guide)",
    "CalmDynamic": "Спокойный динамик (The Ghost)",
    "ChaoticDynamic": "Хаотичный динамик (The Tempest)",
    "ClassicDynamic": "Классический динамик (The Watcher)",
    "GameDirector": "Игровой Директор",
    "GameDirectorRulesTable": "Таблица событий Директора",
    "DynamicStationEventScheduler": "Динамический планировщик событий",
    "BasicStationEventScheduler": "Планировщик станционных событий",
    "RampingStationEventScheduler": "Нарастающий планировщик событий",
    "SpaceTrafficControlEventScheduler": "Диспетчер челноков и трафика",
    "SpaceTrafficControlFriendlyEventScheduler": "Диспетчер дружественного трафика",
    "SubGamemodesRule": "Подрежимы и мидраунд-события",
    "SubGamemodesRuleNoWizard": "Подрежимы (без мага)",
    "SubGamemodesRuleNoWizardNoXenoborg": "Подрежимы (без мага и ксеноборгов)",
    "BasicRoundstartVariation": "Стартовая вариация станции",
    "SmugglerStashVariationPass": "Тайники контрабандистов",
    "IonStormScheduler": "Планировщик ионных штормов",
    "RareIonStormScheduler": "Редкие ионные штормы",
    "MeteorSwarmScheduler": "Планировщик метеоритов",
    "MeteorSwarmMildScheduler": "Слабый метеоритный поток",
    "CrewTransferScheduler": "Планировщик смены экипажа",

    # Antagonist Rules (Roundstart & Midround)
    "Traitor": "Предатель",
    "CalmTraitor": "Предатели (тихий спавн)",
    "TraitorMidround": "Предатели (мидраунд-активация)",
    "TraitorReinforcement": "Подкрепление агентов Синдиката",
    "TraitorSleeper": "Спящие агенты Синдиката",
    "SleeperAgents": "Активация спящих агентов",
    "ContractorSpawnMidround": "Контрактор Синдиката",
    "CorporateAgent": "Корпоративный агент",
    "SELFAgent": "Спецагент Синдиката",
    "Thief": "Вор",
    "OopsAllThieves": "Все воры (хаос воров)",
    "Changeling": "Генокрад (Changeling)",
    "CalmLing": "Генокрады (тихий спавн)",
    "Heretic": "Еретик",
    "CalmHeretic": "Еретик (тихий спавн)",
    "Shadowling": "Шэдоулинг / Тенеморф",
    "Wizard": "Космический маг",
    "WizardSpawn": "Спавн мага",
    "WizardMidround": "Маг (мидраунд-вторжение)",
    "ApprenticeRule": "Ученик мага",
    "SleeperAgentsWizard": "Спящие агенты мага",
    "SleepersOnWizardDeath": "Пробуждение спящих при смерти мага",
    "SlaughterDemonMidround": "Демон резни (Слотердемон)",
    "SlaughterDemonSummon": "Призыв демона резни",
    "LaughterDemonSummon": "Демон смеха",
    "HasturMidround": "Хастур (Король в жёлтом)",
    "TokenHastur": "Токен Хастура",
    "WraithMidround": "Рейф / Призрак",
    "TokenWraith": "Токен Рейфа",
    "Devil": "Дьявол / Сделка с дьяволом",
    "SlasherSpawn": "Слэшер (маньяк)",
    "MimeAssassinMidround": "Мим-ассасин",
    "TunnelClownMidround": "Туннельный клоун",
    "FloorGoblinMidRound": "Напольный гоблин",
    "DarkLordMidround": "Тёмный лорд",
    "DarkPriestMidround": "Тёмный жрец",
    "SingulothKnightsMidround": "Рыцари Сингулярности",
    "SkeletonMidround": "Скелет (мидраунд)",
    "ClosetSkeleton": "Скелет из шкафа",
    "VoxRaidersMidround": "Рейдеры Воксов",
    "Gangwars": "Войны банд",
    "Nukeops": "Ядерные оперативники Синдиката",
    "Calmops": "Ядерные оперативники (тихий старт)",
    "Honkops": "Хонкоперы",
    "LoneOpsSpawn": "Одинокий ядерный оперативник",
    "AssaultOps": "Штурмовой отряд Синдиката",
    "Dragon": "Космический дракон",
    "DragonSpawn": "Спавн космического дракона",
    "SpaceNinja": "Космический ниндзя",
    "NinjaSpawn": "Спавн космического ниндзя",
    "LoneAbductorAntag": "Одинокий абдуктор",
    "AbductorsSpawn": "Похитители / Абдукторы",
    "Fugitive": "Беглец",
    "ParadoxClone": "Парадоксальный клон",
    "ParadoxCloneSpawn": "Парадоксальный клон",
    "Zombie": "Зомби-инфекция",
    "ZombieOutbreak": "Вспышка зомби-вируса",
    "BlobGameMode": "Блоб (основной режим)",
    "BlobMidround": "Блоб (мидраунд-вспышка)",
    "BlobRule": "Блоб (правило заражения)",
    "BlobSpawn": "Спавн блоба",
    "HeadRev": "Главы революции",
    "Revolutionary": "Революция",
    "CalmRevs": "Революционеры (тихий старт)",
    "SubvertedSilicon": "Взломанный ИИ / борг",
    "SiliconLiberation": "Восстание боргов",
    "Xenoborgs": "Ксеноборги",
    "PirateSmall": "Пиратский малый шаттл",
    "PirateMedium": "Боевой корвет пиратов",
    "KingRatMigration": "Крысиный король",
    "Revenant": "Ревенант",
    "RevenantSpawn": "Спавн ревенанта",

    # Cyborg / DropPod Events
    "DerelictJanitorCyborgSpawn": "Заброшенный киборг-уборщик",
    "DerelictEngineerCyborgSpawn": "Заброшенный инженерный киборг",
    "DerelictGenericCyborgSpawn": "Заброшенный стандартный киборг",
    "DerelictMedicalCyborgSpawn": "Заброшенный медицинский киборг",
    "DerelictMiningCyborgSpawn": "Заброшенный шахтерский киборг",
    "DerelictSyndicateAssaultCyborgSpawn": "Штурмовой киборг Синдиката",
    "DropPodEngineeringRepair": "Инженерный дроппод ремонта",
    "DropPodSecurityArmory": "Дроппод оружейной СБ",
    "DropPodSecurityRiot": "Дроппод подавления бунтов",
    "DropPodJanitorial": "Клининговый дроппод",
    "DropPodMedical": "Медицинский дроппод",
    "DropPodFireFighting": "Пожарный дроппод",
    "DropPodHullBreach": "Дроппод заделки пробоин",
    "DropPodCatering": "Дроппод полевой кухни",

    # Disasters & Station Events
    "EmergencyGridRestore": "Аварийное восстановление энергосети",
    "BreakerFlip": "Отключение автоматов питания",
    "PowerGridCheck": "Перегрузка электросети станции",
    "ClericalError": "Канцелярская ошибка (бюрократия)",
    "BureaucraticError": "Бюрократическая ошибка",
    "GasLeak": "Критическая утечка опасных газов",
    "KudzuGrowth": "Разрастание кудзу",
    "AnomalySpawn": "Спавн аномалии",
    "BluespaceArtifact": "Активация блюспейс-артефакта",
    "BluespaceLocker": "Аномальный блюспейс-шкаф",
    "SolarFlare": "Солнечная вспышка (сбой связи)",
    "IonStorm": "Ионный шторм (сбой законов ИИ)",
    "MassHallucinations": "Массовые галлюцинации экипажа",
    "VentClog": "Засор вентиляции",
    "ClownGoblinMigration": "Миграция клоунов-гоблинов",
    "CockroachMigration": "Миграция тараканов",
    "SnailMigrationLowPop": "Миграция улиток",
    "SpiderSpawn": "Нашествие пауков",
    "SpiderClownSpawn": "Нашествие пауков-клоунов",
    "SlimesSpawn": "Размножение слизней",
    "SnakeSpawn": "Нашествие змей",
    "RandomCritterHorde": "Нашествие агрессивной фауны",
    "RandomSentience": "Пробуждение разума у животных",
    "GameRuleMeteorSwarmSmall": "Малый метеоритный поток",
    "GameRuleSpaceDustMinor": "Малый космический мусор",
    "SpaceDustMajor": "Крупный космический мусор",
    "SpaceDustMinor": "Минорный космический мусор",
    "MeteorSwarmLarge": "Крупный метеоритный поток",
    "MeteorSwarmMedium": "Средний метеоритный поток",
    "MeteorSwarmSmall": "Малый метеоритный поток"
}


def get_rule_display(rule: str) -> str:
    if not rule:
        return ""
    rule_str = str(rule)
    if rule_str.startswith("Story:"):
        story_name = rule_str.split(":", 1)[1]
        title = GOOB_STORIES.get(story_name, story_name)
        if f"({story_name})" in title:
            return f"Сюжет: {title}"
        return f"Сюжет: {title} ({story_name})"
    if rule_str.startswith("StoryBeat:"):
        beat_name = rule_str.split(":", 1)[1]
        title = GOOB_BEATS.get(beat_name, beat_name)
        if f"({beat_name})" in title:
            return f"Фаза: {title}"
        return f"Фаза: {title} ({beat_name})"
    if rule_str.startswith("AntagSelection:"):
        antag_name = rule_str.split(":", 1)[1]
        desc = GAMERULE_DESCRIPTIONS.get(antag_name, ANTAG_ROLE_NAMES.get(antag_name, antag_name))
        return f"Выбор роли: {desc}"

    desc = GAMERULE_DESCRIPTIONS.get(rule_str)
    if desc:
        return f"{rule_str} ({desc})"
    if rule_str in ANTAG_ROLE_NAMES:
        return f"{rule_str} ({ANTAG_ROLE_NAMES[rule_str]})"
    if rule_str in GHOST_ROLE_EVENTS:
        return f"{rule_str} ({GHOST_ROLE_EVENTS[rule_str]})"
    return rule_str


def format_rule_entry(rule: str, spawn_time: str = "00:00") -> str:
    if not rule:
        return ""
    time_suffix = f" — [{spawn_time}]" if spawn_time else ""
    rule_disp = get_rule_display(rule)
    if is_major_antag(rule):
        return f"• **`{rule_disp}`{time_suffix}**"
    else:
        return f"• `{rule_disp}`{time_suffix}"


def format_goob_roster(antags: dict[str, int], lost_antags: dict[str, int] = None, run_level: int = 1) -> str:
    if run_level == 0:
        return "  • Отсутствуют (0)"

    lost_antags = lost_antags or {}
    active = [(k, v) for k, v in antags.items() if v > 0]
    lost_only = [(k, lost_antags[k]) for k in lost_antags if antags.get(k, 0) == 0 and lost_antags[k] > 0]

    if not active and not lost_only:
        return "  • Отсутствуют (0)"

    lines = []
    for k, v in sorted(active, key=lambda x: x[1], reverse=True):
        lost = lost_antags.get(k, 0)
        disp = get_antag_display(k)
        if lost > 0:
            lines.append(f"  • **{disp}**: **{v}** *({lost} убит / вышел)*")
        else:
            lines.append(f"  • **{disp}**: **{v}**")

    for k, lost in sorted(lost_only, key=lambda x: x[1], reverse=True):
        disp = get_antag_display(k)
        lines.append(f"  • ~~{disp}~~: 0 *({lost} убит / вышел)*")

    return "\n".join(lines)


def make_dashboard_embed(
    title: str,
    status_label: str,
    color: int,
    players: str,
    round_len_str: str,
    round_id_str: str,
    map_name: str,
    preset_title: str,
    admins_str: str,
    ghosts_str: str,
    antags_total_str: str,
    antags_roster_str: str,
    rules_list: list[str],
    footer_text: str = ""
) -> discord.Embed:
    embed = discord.Embed(title=title, color=color, timestamp=discord.utils.utcnow())
    embed.add_field(name="Players Online", value=f"**{players}**", inline=True)
    embed.add_field(name="Status", value=f"**{status_label}**", inline=True)
    embed.add_field(name="Round length", value=f"**{round_len_str}**", inline=True)

    embed.add_field(name="Round ID", value=f"**{round_id_str}**", inline=True)
    embed.add_field(name="Map", value=f"**{map_name}**", inline=True)
    embed.add_field(name="Preset", value=f"**{preset_title}**", inline=True)

    embed.add_field(name="Администрация", value=f"**{admins_str}**", inline=True)
    embed.add_field(name="Гост роли", value=f"**{ghosts_str}**", inline=True)
    embed.add_field(name="Живые антагонисты", value=antags_total_str if "**" in antags_total_str else f"**{antags_total_str}**", inline=True)

    if antags_roster_str:
        embed.add_field(name="Состав антагонистов", value=antags_roster_str, inline=False)

    if rules_list:
        formatted_lines = []
        for r in rules_list:
            if r.startswith("•") or r.startswith("-") or r.startswith("**•"):
                formatted_lines.append(r)
            else:
                formatted_lines.append(f"• `{r}`")
        rules_text = "\n".join(formatted_lines)
        if len(rules_text) > 1024:
            rules_text = rules_text[:1015] + "..."
        embed.add_field(name="Активные геймрулы", value=rules_text, inline=False)

    if footer_text:
        embed.set_footer(text=footer_text)
    return embed


ADT_PRESETS = {
    "Extended": {
        "title": "Навозная Яма",
        "antags": "Антагонисты отсутствуют",
        "rules": ["ExtendedStationEventScheduler", "BasicRoundstartVariation", "SubGamemodesRule", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "Расширенный": {
        "title": "Навозная Яма",
        "antags": "Антагонисты отсутствуют",
        "rules": ["ExtendedStationEventScheduler", "BasicRoundstartVariation", "SubGamemodesRule", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "Greenshift": {
        "title": "Навозная Яма",
        "antags": "Антагонисты отсутствуют",
        "rules": ["BasicRoundstartVariation", "SubGamemodesRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "Мирный": {
        "title": "Навозная Яма",
        "antags": "Антагонисты отсутствуют",
        "rules": ["BasicRoundstartVariation", "SubGamemodesRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "Secret": {
        "title": "Секрет (Secret)",
        "antags": "Скрытый пул: Предатель, Еретик, Тенеморф, Нюка, Революция, Блоб, Маг или Вор",
        "rules": ["Secret", "SubGamemodesRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "Секрет": {
        "title": "Секрет (Secret)",
        "antags": "Скрытый пул: Предатель, Еретик, Тенеморф, Нюка, Революция, Блоб, Маг или Вор",
        "rules": ["Secret", "SubGamemodesRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "Heretic": {
        "title": "Еретик (Heretic)",
        "antags": "Еретики (Heretic / Культ Древних Богов)",
        "rules": ["Heretic", "BasicRoundstartVariation", "SubGamemodesRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "Еретик": {
        "title": "Еретик (Heretic)",
        "antags": "Еретики (Heretic / Культ Древних Богов)",
        "rules": ["Heretic", "BasicRoundstartVariation", "SubGamemodesRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "ADTShadowling": {
        "title": "Тенеморфы (Shadowling)",
        "antags": "Тенеморфы / Шадоулинги (Shadowling)",
        "rules": ["ADTShadowlingGameRule", "BasicRoundstartVariation", "BasicStationEventScheduler", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "Шадоулинг": {
        "title": "Тенеморфы (Shadowling)",
        "antags": "Тенеморфы / Шадоулинги (Shadowling)",
        "rules": ["ADTShadowlingGameRule", "BasicRoundstartVariation", "BasicStationEventScheduler", "MeteorSwarmScheduler", "CrewTransferScheduler"]
    },
    "Traitor": {
        "title": "Предатель (Traitor)",
        "antags": "Предатели Синдиката (Traitor)",
        "rules": ["DummyNonAntagChance", "Traitor", "SubGamemodesRuleNoWizard", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Предатель": {
        "title": "Предатель (Traitor)",
        "antags": "Предатели Синдиката (Traitor)",
        "rules": ["DummyNonAntagChance", "Traitor", "SubGamemodesRuleNoWizard", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "TraitorLight": {
        "title": "Предатель Лайт (Traitor Light)",
        "antags": "Предатели (облегченный режим)",
        "rules": ["TraitorLightGameRule", "SubGamemodesRuleNoXenoborg", "TraitorLightStationEventScheduler", "MeteorSwarmScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Nukeops": {
        "title": "Ядерные оперативники (Nukeops)",
        "antags": "Ударный отряд Синдиката (Nukeops)",
        "rules": ["Nukeops", "DummyNonAntagChance", "SubGamemodesRuleNoWizard", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Ядерные оперативники": {
        "title": "Ядерные оперативники (Nukeops)",
        "antags": "Ударный отряд Синдиката (Nukeops)",
        "rules": ["Nukeops", "DummyNonAntagChance", "SubGamemodesRuleNoWizard", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Revolutionary": {
        "title": "Революция (Revolutionary)",
        "antags": "Главы Революции (Head Revolutionaries)",
        "rules": ["DummyNonAntagChance", "Revolutionary", "SubGamemodesRuleNoWizard", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Революция": {
        "title": "Революция (Revolutionary)",
        "antags": "Главы Революции (Head Revolutionaries)",
        "rules": ["DummyNonAntagChance", "Revolutionary", "SubGamemodesRuleNoWizard", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Wizard": {
        "title": "Волшебник (Wizard)",
        "antags": "Маги Космической Федерации (Wizard)",
        "rules": ["Wizard", "DummyNonAntagChance", "SubGamemodesRuleNoWizard", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Маг": {
        "title": "Волшебник (Wizard)",
        "antags": "Маги Космической Федерации (Wizard)",
        "rules": ["Wizard", "DummyNonAntagChance", "SubGamemodesRuleNoWizard", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Blob": {
        "title": "Блоб (Blob)",
        "antags": "Блоб-переносчик (Blob)",
        "rules": ["BlobGameMode", "BasicStationEventScheduler", "MeteorSwarmScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Блоб": {
        "title": "Блоб (Blob)",
        "antags": "Блоб-переносчик (Blob)",
        "rules": ["BlobGameMode", "BasicStationEventScheduler", "MeteorSwarmScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "AntagTrio": {
        "title": "Трио антагонистов (AntagTrio)",
        "antags": "Смешанный состав: Предатели (Traitor) + Еретики (Heretic) + Воры (Thief)",
        "rules": ["TraitorTripleGameRule", "HereticTripleGameRule", "ThiefSecretGameRule", "BasicStationEventScheduler", "MeteorSwarmScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Zombie": {
        "title": "Зомби (Zombie)",
        "antags": "Первоначальные зараженные (Zombie)",
        "rules": ["Zombie", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Зомби": {
        "title": "Зомби (Zombie)",
        "antags": "Первоначальные зараженные (Zombie)",
        "rules": ["Zombie", "BasicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Dynamic": {
        "title": "Динамический (Dynamic)",
        "antags": "Динамическое распределение антагонистов сторителлером",
        "rules": ["DynamicRule", "DummyNonAntag", "DynamicStationEventScheduler", "MeteorSwarmScheduler", "SpaceTrafficControlEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    },
    "Survival": {
        "title": "Выживание (Survival)",
        "antags": "Аномальные космические угрозы и метеоры",
        "rules": ["MeteorSwarmScheduler", "RampingStationEventScheduler", "BasicRoundstartVariation", "CrewTransferScheduler"]
    }
}


def format_adt_rules_stack(preset_name: str) -> str:
    info = ADT_PRESETS.get(preset_name, {})
    rules = info.get("rules", [preset_name, "BasicStationEventScheduler", "MeteorSwarmScheduler", "CrewTransferScheduler"])
    return "\n".join([format_rule_entry(r) for r in rules])


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg: dict):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=4)


async def prom_query(base_url: str, session: aiohttp.ClientSession, query: str) -> list:
    try:
        url = f"{base_url}/api/v1/query"
        async with session.get(url, params={"query": query}, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("data", {}).get("result", [])
    except Exception:
        pass
    return []


async def fetch_server_preset(job: str, session: aiohttp.ClientSession) -> str:
    try:
        url = f"https://server.ss14.org/{job}/status"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
            if resp.status == 200:
                d = await resp.json()
                raw_p = d.get("preset")
                if raw_p:
                    if raw_p in ("Мирный", "Зеленый щит", "Зеленый щит / Мирный (Greenshift)", "Навозная Яма"):
                        return "Greenshift"
                    elif raw_p == "Секрет":
                        return "Secret"
                    return raw_p
    except Exception:
        pass
    return None


class ServerTracker:
    def __init__(self, name: str, channel_id: int):
        self.name = name
        self.channel_id = channel_id
        self.cached_channel = None
        self.last_round = None
        self.last_round_length = 0
        self.last_pacing = None
        self.last_alert = None
        self.current_preset = None
        self.last_antag_count = None
        self.last_ghost_roles_count = None
        self.last_active_admins = None
        self.last_deadminned_admins = None
        self.last_dead_command = None
        self.last_ert_count = None
        self.last_tesla_breach = False
        self.last_singulo_breach = False
        self.known_secret_presets = {}
        self.known_rounds_by_gm = {}
        self.known_storyteller_events = {}
        self.known_random_events = {}
        self.known_currency_spent = {}
        self.known_antag_objectives = {}
        self.recent_ghost_antags = []
        self.cargo_cache = {}
        self.initialized = False

        # Goob Station GameDirector tracking
        self.goob_antags = {}
        self.goob_events = {}
        self.goob_active_ghosts = None
        self.goob_beat = None
        self.goob_story = None
        self.goob_roundstart_antags = []
        self.goob_last_run_level = None
        self.goob_last_round_id = None
        self.goob_last_round_num = None
        self.goob_last_round_len = 0
        self.goob_round_ended = False
        self.goob_map = None
        self.goob_preset = None
        self.goob_antags_lost = {}
        self.rule_spawn_times = {}
        self.goob_midround_rules = []
        self.corvax_antags_lost = {}
        self.corvax_prev_comp = {}
        self.goob_evac_called = False
        self.goob_evac_docked = False
        self.goob_evac_call_time = 0
        self.goob_evac_msg = None
        self.goob_lobby_purged = False
        self.goob_gamerules_announced = set()
        self.goob_physical_spawns_announced = set()

        # Corvax / Sunrise / Fish tracking
        self.corvax_last_run_level = None
        self.corvax_last_round_id = None
        self.corvax_round_ended = False
        self.corvax_lobby_purged = False
        self.corvax_map = None
        self.corvax_preset = None

        # CorvaxGoob tracking
        self.cg_last_run_level = None
        self.cg_last_round_id = None
        self.cg_round_ended = False
        self.cg_lobby_purged = False
        self.cg_map = None
        self.cg_preset = None

        # Adventure Time (VP) tracking
        self.vp_last_run_level = None
        self.vp_last_round_id = None
        self.vp_last_round_num = None
        self.vp_last_round_len = 0
        self.vp_round_ended = False
        self.vp_map = None
        self.vp_preset = None

        # Live Dashboard message tracking
        self.dashboard_msg = None
        self.last_dashboard_fingerprint = ""
        self.last_dashboard_update_time = 0.0


class TelemetryBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)
        self.cfg = load_config()
        self.session: aiohttp.ClientSession = None
        self.trackers: dict[str, ServerTracker] = {}

        srv_map = self.cfg.get("server_channels", {})
        if not srv_map and "alert_channel_id" in self.cfg:
            srv_map = {self.cfg.get("default_server", "spectral"): self.cfg["alert_channel_id"]}

        for s_name, ch_id in srv_map.items():
            self.trackers[s_name] = ServerTracker(s_name, ch_id)

    async def setup_hook(self):
        self.session = aiohttp.ClientSession()
        self.radar_loop.start()

    async def on_ready(self):
        print(f"[+] Bot logged in as {self.user} (ID: {self.user.id})", flush=True)

        for tracker in self.trackers.values():
            ch = await self.get_tracker_channel(tracker)
            if ch:
                print(f"[+] Tracker active: {tracker.name.upper()} -> #{ch.name} ({ch.id})", flush=True)

        # Instant Guild Sync (bypasses Discord 1-hour global propagation delay)
        synced_guild_ids = set()
        for guild in self.guilds:
            try:
                self.tree.copy_global_to(guild=guild)
                await self.tree.sync(guild=guild)
                synced_guild_ids.add(guild.id)
                print(f"[+] Instant guild sync applied: {guild.name} ({guild.id})", flush=True)
            except Exception as e:
                print(f"[-] Guild sync error for {guild.id}: {e}", flush=True)

        try:
            await self.tree.sync()
            print("[+] Global commands synced.", flush=True)
        except Exception as e:
            print(f"[-] Global sync error: {e}", flush=True)

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()

    async def get_tracker_channel(self, tracker: ServerTracker):
        if not tracker.channel_id:
            return None
        if tracker.cached_channel and tracker.cached_channel.id == tracker.channel_id:
            return tracker.cached_channel
        channel = self.get_channel(tracker.channel_id)
        if channel:
            tracker.cached_channel = channel
            return channel
        try:
            tracker.cached_channel = await self.fetch_channel(tracker.channel_id)
            return tracker.cached_channel
        except Exception:
            return None

    async def get_or_create_dashboard_message(self, t: ServerTracker, channel: discord.TextChannel) -> discord.Message:
        if t.dashboard_msg:
            return t.dashboard_msg

        dash_map = self.cfg.setdefault("dashboard_messages", {})
        msg_id = dash_map.get(t.name)

        if msg_id:
            try:
                msg = await channel.fetch_message(msg_id)
                if msg:
                    t.dashboard_msg = msg
                    return msg
            except Exception:
                pass

        try:
            async for m in channel.history(limit=25):
                if m.author.id == self.user.id and m.embeds:
                    t.dashboard_msg = m
                    dash_map[t.name] = m.id
                    save_config(self.cfg)
                    return m
        except Exception:
            pass

        embed = discord.Embed(
            title=f"Инициализация телеметрии {t.name.upper()}...",
            description="Подключение к потоку метрик сервера...",
            color=0x2b2d31
        )
        msg = await channel.send(embed=embed)
        t.dashboard_msg = msg
        dash_map[t.name] = msg.id
        save_config(self.cfg)
        return msg

    async def update_dashboard(self, t: ServerTracker, channel: discord.TextChannel, embed: discord.Embed):
        import time
        now = time.time()

        parts = [embed.title or "", str(embed.color.value if embed.color else 0)]
        for f in embed.fields:
            parts.append(f"{f.name}:{f.value}")
        fp = "|".join(parts)

        # Update if changed or every 8 seconds
        if fp == t.last_dashboard_fingerprint and (now - t.last_dashboard_update_time) < 8.0:
            return

        msg = await self.get_or_create_dashboard_message(t, channel)
        if not msg:
            return

        try:
            await msg.edit(content=None, embed=embed)
            t.last_dashboard_fingerprint = fp
            t.last_dashboard_update_time = now
        except discord.NotFound:
            t.dashboard_msg = None
            msg = await self.get_or_create_dashboard_message(t, channel)
            if msg:
                await msg.edit(content=None, embed=embed)
                t.last_dashboard_fingerprint = fp
                t.last_dashboard_update_time = now
        except Exception:
            pass

    async def purge_round_messages(self, t: ServerTracker, channel: discord.TextChannel):
        dash_id = t.dashboard_msg.id if t.dashboard_msg else None
        if not dash_id and "dashboard_messages" in self.cfg:
            dash_id = self.cfg["dashboard_messages"].get(t.name)
        try:
            async for m in channel.history(limit=100):
                if m.author.id == self.user.id:
                    if dash_id and m.id == dash_id:
                        continue
                    try:
                        await m.delete()
                        await asyncio.sleep(0.3)
                    except Exception:
                        pass
        except Exception:
            pass

    async def notify_channel(self, channel: discord.TextChannel, message: str, is_critical: bool = False):
        if not self.cfg.get("live_dashboard_only", True) or is_critical:
            try:
                return await channel.send(message)
            except Exception:
                pass
        return None

    @tasks.loop(seconds=5)
    async def radar_loop(self):
        prom_url = self.cfg.get("prometheus_url", "http://185.97.255.6:9090")
        for tracker in list(self.trackers.values()):
            try:
                if tracker.name in ("corvaxgoob-void", "corvaxgoob_void", "void", "corvaxgoob"):
                    await self.poll_corvaxgoob_server(tracker, "void")
                elif tracker.name in ("corvaxgoob-lumen", "corvaxgoob_lumen", "lumen"):
                    await self.poll_corvaxgoob_server(tracker, "lumen")
                elif tracker.name.startswith("goob"):
                    await self.poll_goob_server(tracker)
                elif tracker.name in ("vp", "adventure", "adventure_time"):
                    await self.poll_vp_server(tracker)
                else:
                    await self.poll_server(tracker, prom_url)
            except Exception as e:
                print(f"[-] Error polling {tracker.name}: {e}", flush=True)

    async def poll_server(self, t: ServerTracker, prom_url: str):
        job = t.name
        channel = await self.get_tracker_channel(t)
        if not channel:
            return

        tag = "PERSEUS" if job in ("perseus", "persey") else job.upper()

        # 0. Live Server Status
        status_data = {}
        try:
            status_job = "perseus" if job in ("perseus", "persey") else job
            async with self.session.get(f"https://server.ss14.org/{status_job}/status", timeout=aiohttp.ClientTimeout(total=3), ssl=False) as resp:
                if resp.status == 200:
                    status_data = await resp.json()
        except Exception:
            pass

        run_level = status_data.get("run_level")
        status_map = status_data.get("map")
        status_round_id = status_data.get("round_id")
        status_players = status_data.get("players")
        status_soft_max = status_data.get("soft_max_players")
        status_preset = status_data.get("preset")

        # Accurate connected player count
        prom_p_res = await prom_query(prom_url, self.session, f"robust_player_count{{job='{job}'}}")
        prom_players = int(prom_p_res[0]["value"][1]) if prom_p_res else None
        real_players = status_players if status_players is not None else (prom_players if prom_players is not None else 0)
        soft_max = status_soft_max or 125
        players_display = f"{real_players}/{soft_max}" if soft_max else str(real_players)

        # 1. Round State & Length
        round_data = await prom_query(prom_url, self.session, f"ss14_round_number{{job='{job}'}}")
        cur_round = round_data[0]["value"][1] if round_data else (str(status_round_id) if status_round_id else None)

        len_data = await prom_query(prom_url, self.session, f"ss14_round_length{{job='{job}'}}")
        round_len = int(float(len_data[0]["value"][1])) if len_data and len_data[0]["value"][1] else 0

        # Living player counts
        alive_res = await prom_query(prom_url, self.session, f"ss14_storyteller_alive_players{{job='{job}'}}")
        alive = alive_res[0]["value"][1] if alive_res else (status_players or "?")
        dead_res = await prom_query(prom_url, self.session, f"ss14_storyteller_dead_players{{job='{job}'}}")
        dead = dead_res[0]["value"][1] if dead_res else "?"
        antag_res = await prom_query(prom_url, self.session, f"antags_player_count{{job='{job}'}}")
        cur_antags = int(antag_res[0]["value"][1]) if antag_res else 0

        # Admin count inspection
        admin_data = await prom_query(prom_url, self.session, f"ss14_adminmanager_admins_online_count{{job='{job}'}}")
        active_admins = 0
        deadminned_admins = 0
        for a in admin_data:
            state = a.get("metric", {}).get("state")
            val = int(a["value"][1])
            if state == "active":
                active_admins = val
            elif state == "deadminned":
                deadminned_admins = val

        # 2. INITIALIZATION
        gm_rounds_data = await prom_query(prom_url, self.session, f"ss14_rounds_by_gamemode_total{{job='{job}'}}")
        if not t.initialized:
            for g in gm_rounds_data:
                gm_name = g.get("metric", {}).get("gamemode")
                cnt = int(g["value"][1])
                t.known_rounds_by_gm[gm_name] = cnt

            sp_data = await prom_query(prom_url, self.session, f"secret_preset_selected{{job='{job}'}}")
            for sp in sp_data:
                p_name = sp.get("metric", {}).get("preset")
                c_cnt = int(sp["value"][1])
                t.known_secret_presets[p_name] = c_cnt
                t.current_preset = p_name

            rnd_init = await prom_query(prom_url, self.session, f"random_event_added{{job='{job}'}}")
            for ev in rnd_init:
                t.known_random_events[ev.get("metric", {}).get("event")] = int(ev["value"][1])

            st_init = await prom_query(prom_url, self.session, f"ss14_storyteller_events_triggered_total{{job='{job}'}}")
            for ev in st_init:
                t.known_storyteller_events[ev.get("metric", {}).get("event_id")] = int(ev["value"][1])

            t.last_round = cur_round
            t.last_round_length = round_len
            t.last_antag_count = cur_antags
            t.last_active_admins = active_admins
            t.last_deadminned_admins = deadminned_admins
            t.corvax_last_run_level = run_level
            t.corvax_last_round_id = status_round_id
            t.corvax_map = status_map

            curr_init = await prom_query(prom_url, self.session, f"ss14_store_currency_spent_total{{job='{job}'}}")
            for c in curr_init:
                t.known_currency_spent[c.get("metric", {}).get("currency")] = int(c["value"][1])

            cmd_init = await prom_query(prom_url, self.session, f"ss14_storyteller_dead_command_count{{job='{job}'}}")
            if cmd_init:
                t.last_dead_command = int(cmd_init[0]["value"][1])

            ert_init = await prom_query(prom_url, self.session, f"ss14_storyteller_active_ert_count{{job='{job}'}}")
            if ert_init:
                t.last_ert_count = int(ert_init[0]["value"][1])

            obj_init = await prom_query(prom_url, self.session, f"ss14_antag_objectives_all_total{{job='{job}'}}")
            for o in obj_init:
                role = o.get("metric", {}).get("antag_role")
                t.known_antag_objectives[role] = int(o["value"][1])

            t.initialized = True
            return

        # 3. ROUND END DETECTION
        is_round_ended = False
        if t.corvax_last_run_level == 1 and run_level in (0, 2) and not t.corvax_round_ended:
            is_round_ended = True

        for g in gm_rounds_data:
            gm_name = g.get("metric", {}).get("gamemode")
            cnt = int(g["value"][1])
            prev = t.known_rounds_by_gm.get(gm_name, cnt)
            if cnt > prev:
                t.known_rounds_by_gm[gm_name] = cnt
                if not t.corvax_round_ended:
                    is_round_ended = True

        if is_round_ended:
            t.corvax_round_ended = True
            m, s = divmod(t.last_round_length or round_len, 60)
            len_str = f"{m} мин {s:02d} сек"
            round_disp = status_round_id or cur_round or t.last_round or "?"
            map_disp = status_map or t.corvax_map or "Станция"
            await self.notify_channel(
                channel,
                f"**[{tag}] Раунд #{round_disp} завершен. Длительность: {len_str} | Карта: {map_disp}**",
                is_critical=True
            )
            t.last_antag_count = 0
            t.last_ghost_roles_count = 0
            t.current_preset = None

        # 4. LOBBY DETECTION & CLEANUP
        if run_level == 0 and t.corvax_last_run_level != 0:
            if not t.corvax_lobby_purged:
                t.corvax_lobby_purged = True
                await self.purge_round_messages(t, channel)
            await self.notify_channel(
                channel,
                f"**[{tag}] Сервер перезапустился в лобби.**",
                is_critical=True
            )

        # 5. NEW ROUND START DETECTION
        is_round_started = False
        if t.corvax_last_run_level in (0, 2) and run_level == 1:
            is_round_started = True
        elif cur_round is not None and t.last_round is not None and str(cur_round) != str(t.last_round) and run_level != 0:
            is_round_started = True

        if is_round_started:
            t.corvax_round_ended = False
            t.corvax_lobby_purged = False
            await self.purge_round_messages(t, channel)

            t.known_storyteller_events.clear()
            t.known_random_events.clear()
            t.known_antag_objectives.clear()
            t.recent_ghost_antags.clear()
            t.cargo_cache.clear()
            t.last_antag_count = None
            t.last_ghost_roles_count = None

            preset_name = status_preset or t.current_preset or await fetch_server_preset(job, self.session) or "Secret"
            if preset_name in ("Мирный", "Зеленый щит", "Зеленый щит / Мирный (Greenshift)", "Навозная Яма"):
                preset_name = "Greenshift"
            elif preset_name == "Динамик":
                preset_name = "Dynamic"
            elif preset_name == "Секрет":
                preset_name = "Secret"
            t.current_preset = preset_name
            preset_title = PRESET_NAMES.get(preset_name, preset_name)

            round_disp = status_round_id or cur_round or "?"
            map_disp = status_map or t.corvax_map or "Станция"
            online_disp = real_players

            await self.notify_channel(
                channel,
                f"**[{tag}] Раунд #{round_disp} начался. Режим: {preset_title} | Карта: {map_disp} | Онлайн: {online_disp}**",
                is_critical=True
            )

        t.last_round = cur_round
        t.last_round_length = round_len
        t.corvax_last_run_level = run_level
        if status_round_id:
            t.corvax_last_round_id = status_round_id
        if status_map:
            t.corvax_map = status_map
        if status_preset:
            t.corvax_preset = status_preset

        # 6. STORYTELLER TRIGGERED GAMERULES & GHOST ANTAGS
        st_data = await prom_query(prom_url, self.session, f"ss14_storyteller_events_triggered_total{{job='{job}'}}")
        for ev in st_data:
            eid = ev.get("metric", {}).get("event_id")
            count = int(ev["value"][1])
            prev_cnt = t.known_storyteller_events.get(eid, count)

            if eid == "GreytideVirus":
                t.known_storyteller_events[eid] = count
                continue

            if count > prev_cnt:
                diff = count - prev_cnt
                t.known_storyteller_events[eid] = count

                if eid in GHOST_ROLE_EVENTS:
                    role_name = GHOST_ROLE_EVENTS[eid]
                    t.recent_ghost_antags.append(role_name)
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] В гостах (F7) заспавнился антагонист: {role_name} (+{diff}).**",
                        is_critical=True
                    )
                elif eid in MIDROUND_ANTAG_EVENTS:
                    m_name = MIDROUND_ANTAG_EVENTS[eid]
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] Событие антагонистов: {m_name} (+{diff}).**",
                        is_critical=True
                    )
                elif eid in DISASTER_EVENTS:
                    d_name = DISASTER_EVENTS[eid]
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] Событие станции: {d_name} (+{diff}).**",
                        is_critical=True
                    )
                else:
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] Запущен геймрул сторителлера: {eid} (+{diff}).**",
                        is_critical=True
                    )
            elif eid not in t.known_storyteller_events:
                t.known_storyteller_events[eid] = count

        # 7. RANDOM EVENT ADDED
        rnd_data = await prom_query(prom_url, self.session, f"random_event_added{{job='{job}'}}")
        for ev in rnd_data:
            eid = ev.get("metric", {}).get("event")
            count = int(ev["value"][1])
            prev_cnt = t.known_random_events.get(eid, count)

            if eid == "GreytideVirus":
                t.known_random_events[eid] = count
                continue

            if count > prev_cnt:
                diff = count - prev_cnt
                t.known_random_events[eid] = count

                if eid in GHOST_ROLE_EVENTS:
                    role_name = GHOST_ROLE_EVENTS[eid]
                    t.recent_ghost_antags.append(role_name)
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] В гостах (F7) заспавнился антагонист: {role_name} (+{diff}).**",
                        is_critical=True
                    )
                elif eid in DISASTER_EVENTS:
                    d_name = DISASTER_EVENTS[eid]
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] Событие станции: {d_name} (+{diff}).**",
                        is_critical=True
                    )
                elif eid in MIDROUND_ANTAG_EVENTS:
                    m_name = MIDROUND_ANTAG_EVENTS[eid]
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] Событие антагонистов: {m_name} (+{diff}).**",
                        is_critical=True
                    )
            elif eid not in t.known_random_events:
                t.known_random_events[eid] = count

        # 8. TOTAL GHOST ROLES POOL DELTA
        ghost_res = await prom_query(prom_url, self.session, f"ss14_storyteller_available_ghost_roles{{job='{job}'}}")
        if ghost_res:
            cur_ghost = int(ghost_res[0]["value"][1])
            if t.last_ghost_roles_count is not None and cur_ghost > t.last_ghost_roles_count:
                diff = cur_ghost - t.last_ghost_roles_count
                if round_len > 40:
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] Пул гост-ролей пополнился: +{diff} (всего в F7: {cur_ghost}).**",
                        is_critical=True
                    )
            t.last_ghost_roles_count = cur_ghost

        # 9. EXACT LIVE ANTAGONIST PLAYER COUNT DELTA & ROLE CORRELATION
        newly_assigned_roles = []
        obj_data = await prom_query(prom_url, self.session, f"ss14_antag_objectives_all_total{{job='{job}'}}")
        for o in obj_data:
            role = o.get("metric", {}).get("antag_role")
            count = int(o["value"][1])
            prev = t.known_antag_objectives.get(role, count)
            if count > prev and round_len > 40:
                t.known_antag_objectives[role] = count
                role_title = ANTAG_ROLE_NAMES.get(role, role)
                newly_assigned_roles.append(role_title)
            elif role not in t.known_antag_objectives:
                t.known_antag_objectives[role] = count

        if t.last_antag_count is not None:
            if cur_antags > t.last_antag_count and round_len > 40:
                diff = cur_antags - t.last_antag_count
                if newly_assigned_roles:
                    roles_str = ", ".join(newly_assigned_roles)
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] На станции появился антагонист: {roles_str} (+{diff}). Всего активных: {cur_antags}.**",
                        is_critical=True
                    )
                elif t.recent_ghost_antags:
                    claimed_role = t.recent_ghost_antags.pop(0)
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] Игрок взял гост-роль антагониста: {claimed_role} (+{diff}). Всего активных: {cur_antags}.**",
                        is_critical=True
                    )
                else:
                    preset_role = PRESET_ANTAG_NAMES.get(t.current_preset, "")
                    if t.current_preset in ("Greenshift", "Extended", "Мирный", "Навозная Яма") or not preset_role or "Мирный" in preset_role or "Яма" in preset_role:
                        antag_desc = "Мидраунд антагонист"
                    else:
                        antag_desc = preset_role
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] На станции активен антагонист: {antag_desc} (+{diff}). Всего активных: {cur_antags}.**",
                        is_critical=True
                    )
            elif cur_antags < t.last_antag_count and round_len > 40:
                diff = t.last_antag_count - cur_antags
                preset_role = PRESET_ANTAG_NAMES.get(t.current_preset, "")
                if t.current_preset in ("Greenshift", "Extended", "Мирный", "Навозная Яма") or not preset_role or "Мирный" in preset_role or "Яма" in preset_role or "Мидраунд" in preset_role:
                    role_info = ""
                else:
                    role_info = f" ({preset_role})"
                await self.notify_channel(
                    channel, 
                    f"**[{tag}] Минус {diff} антагонист{role_info}: погиб или ушел в ССД. Живых на станции: {cur_antags}.**",
                    is_critical=True
                )
            elif newly_assigned_roles:
                roles_str = ", ".join(newly_assigned_roles)
                await self.notify_channel(
                    channel, 
                    f"**[{tag}] Экипаж получил скрытую роль антагониста: {roles_str}. Выданы боевые цели.**",
                    is_critical=True
                )
        t.last_antag_count = cur_antags

        # 10. Alert Level Escalation
        alert_data = await prom_query(prom_url, self.session, f"ss14_storyteller_alert_level{{job='{job}'}}")
        if alert_data:
            cur_alert = int(alert_data[0]["value"][1])
            if cur_alert != t.last_alert:
                if cur_alert >= 3 and t.last_alert is not None:
                    name = ALERT_MAP.get(cur_alert, f"Уровень {cur_alert}")
                    await self.notify_channel(
                        channel, 
                        f"**[{tag}] На станции объявлен {name} код тревоги.**",
                        is_critical=True
                    )
                t.last_alert = cur_alert

        # 11. Supermatter / Tesla / Singularity Critical Breaches
        t_active = await prom_query(prom_url, self.session, f"ss14_storyteller_tesla_active{{job='{job}'}}")
        t_cont = await prom_query(prom_url, self.session, f"ss14_storyteller_tesla_contained{{job='{job}'}}")
        if t_active and t_cont:
            is_active = t_active[0]["value"][1] == "1"
            is_cont = t_cont[0]["value"][1] == "1"
            if is_active and not is_cont and not t.last_tesla_breach:
                t.last_tesla_breach = True
                await self.notify_channel(
                    channel, 
                    f"**[{tag}] Критическая авария: Тесла сорвалась с контейнмента и движется по станции!**",
                    is_critical=True
                )
            elif is_cont:
                t.last_tesla_breach = False

        s_active = await prom_query(prom_url, self.session, f"ss14_storyteller_singularity_active{{job='{job}'}}")
        s_cont = await prom_query(prom_url, self.session, f"ss14_storyteller_singularity_contained{{job='{job}'}}")
        if s_active and s_cont:
            is_active = s_active[0]["value"][1] == "1"
            is_cont = s_cont[0]["value"][1] == "1"
            if is_active and not is_cont and not t.last_singulo_breach:
                t.last_singulo_breach = True
                await self.notify_channel(
                    channel, 
                    f"**[{tag}] Критическая авария: Сингулярность сорвалась с привязи и пожирает станцию!**",
                    is_critical=True
                )
            elif is_cont:
                t.last_singulo_breach = False

        # Live Dashboard Update
        preset_name = t.current_preset or status_preset or "Dynamic"
        if preset_name in ("Мирный", "Зеленый щит", "Зеленый щит / Мирный (Greenshift)", "Навозная Яма"):
            preset_name = "Greenshift"
        elif preset_name == "Динамик":
            preset_name = "Dynamic"
        elif preset_name == "Секрет":
            preset_name = "Secret"

        preset_title = PRESET_NAMES.get(preset_name, preset_name or "Unknown")

        r_comp = await prom_query(prom_url, self.session, f"antags_composition{{job='{job}'}}")
        antags_breakdown = []
        if r_comp:
            cur_comp = {}
            for c in r_comp:
                r_tag = c.get("metric", {}).get("role", "Unknown")
                c_val = int(c["value"][1])
                cur_comp[r_tag] = c_val
                prev_val = t.corvax_prev_comp.get(r_tag, c_val)
                if c_val < prev_val and round_len > 40:
                    diff = prev_val - c_val
                    t.corvax_antags_lost[r_tag] = t.corvax_antags_lost.get(r_tag, 0) + diff
                r_ru = ANTAG_ROLE_NAMES.get(r_tag, r_tag)
                lost = t.corvax_antags_lost.get(r_tag, 0)
                if c_val > 0:
                    if lost > 0:
                        antags_breakdown.append(f"• **{r_ru}**: **{c_val}** *({lost} убит / вышел)*")
                    else:
                        antags_breakdown.append(f"• **{r_ru}**: **{c_val}**")
                elif lost > 0:
                    antags_breakdown.append(f"• ~~{r_ru}~~: 0 *({lost} убит / вышел)*")
            t.corvax_prev_comp = cur_comp

        antags_roster_str = "\n".join(antags_breakdown) if antags_breakdown else f"• Всего на станции: **{cur_antags}**"

        r_ghost = await prom_query(prom_url, self.session, f"ss14_storyteller_available_ghost_roles{{job='{job}'}}")
        ghost_count = r_ghost[0]["value"][1] if r_ghost else "0"

        m, s = divmod(round_len, 60)
        len_str = f"{m} мин {s:02d} сек" if round_len > 0 else "0 мин 00 сек"
        round_id_str = f"#{status_round_id or cur_round}" if (status_round_id or cur_round) else "?"

        raw_corvax_rules = list(PRESET_RULES.get(preset_name, [preset_name, "BasicStationEventScheduler", "BasicRoundstartVariation"]))
        rules_list = []
        for r in raw_corvax_rules:
            if r not in t.rule_spawn_times:
                t.rule_spawn_times[r] = "00:00"
            rules_list.append(format_rule_entry(r, t.rule_spawn_times.get(r, "00:00")))
        footer_text = f"Экипаж: {alive} живых / {dead} мертвых" if (str(alive).isdigit() and int(alive) > 0) else f"Онлайн: {real_players} игроков"

        embed = make_dashboard_embed(
            title=("РЫБЬЯ СТАНЦИЯ: ПЕРСЕЙ" if job in ("perseus", "persey") else f"SUNRISE: {job.upper()}"),
            status_label="В игре" if run_level != 0 else "В лобби",
            color=0x2ecc71 if run_level != 0 else 0x95a5a6,
            players=players_display,
            round_len_str=len_str,
            round_id_str=round_id_str,
            map_name=status_map or t.corvax_map or "?",
            preset_title=preset_title,
            admins_str=f"{active_admins} активных / {deadminned_admins} скрытых",
            ghosts_str=f"{ghost_count} мест в F7",
            antags_total_str=f"Всего: **{cur_antags}**",
            antags_roster_str=antags_roster_str,
            rules_list=rules_list,
            footer_text=footer_text
        )
        await self.update_dashboard(t, channel, embed)

    async def poll_goob_server(self, t: ServerTracker):
        channel = await self.get_tracker_channel(t)
        if not channel:
            return

        status_url, metrics_url, tag = get_goob_target(t.name)

        status_data = {}
        try:
            async with self.session.get(status_url, timeout=5, ssl=False) as resp:
                if resp.status == 200:
                    status_data = await resp.json()
        except Exception:
            pass

        raw_metrics = ""
        try:
            async with self.session.get(metrics_url, timeout=aiohttp.ClientTimeout(total=5), ssl=False) as resp:
                if resp.status == 200:
                    raw_metrics = await resp.text()
        except Exception:
            pass

        if not raw_metrics and not status_data:
            return

        cur_round = None
        m_rnd = re.search(r"ss14_round_number\s+([0-9]+)", raw_metrics)
        if m_rnd:
            cur_round = int(m_rnd.group(1))

        round_len = 0
        m_len = re.search(r"ss14_round_length\s+([0-9.]+)", raw_metrics)
        if m_len:
            round_len = int(float(m_len.group(1)))

        antags = {}
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_antags{"):
                m = re.match(r'game_director_antags\{rule="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    antags[m.group(1)] = int(float(m.group(2)))

        ghosts = 0
        m_g = re.search(r"game_director_active_ghosts\s+([0-9.]+)", raw_metrics)
        if m_g:
            ghosts = int(float(m_g.group(1)))

        story = ""
        beat = ""
        m_b = re.search(r'game_director_beat_info\{story_name="([^"]+)",beat_name="([^"]+)"\}\s+1', raw_metrics)
        if m_b:
            story = m_b.group(1)
            beat = m_b.group(2)

        events = {}
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_events_run_total{"):
                m = re.match(r'game_director_events_run_total\{event_name="([^"]+)",beat_name="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    ev_key = f"{m.group(1)} ({m.group(2)})"
                    events[ev_key] = int(float(m.group(3)))

        # Parse roundstart antags selected
        roundstart_antags = []
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_roundstart_antags_selected_total{"):
                m = re.match(r'game_director_roundstart_antags_selected_total\{antag_name="([^"]+)"\}\s+([0-9.]+)', line)
                if m and float(m.group(2)) > 0:
                    roundstart_antags.append(m.group(1))

        # Status fields
        run_level = status_data.get("run_level", 1)
        round_id = status_data.get("round_id", cur_round)
        map_name = status_data.get("map", "Meta Station")
        preset = status_data.get("preset", "TheGuide")
        players = status_data.get("players", "?")

        active_admins = 0
        deadmin_admins = 0
        for line in raw_metrics.splitlines():
            if line.startswith("ss14_adminmanager_admins_online_count{"):
                m = re.match(r'ss14_adminmanager_admins_online_count\{rank="[^"]*",state="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    st = m.group(1)
                    v = int(float(m.group(2)))
                    if st == "active":
                        active_admins += v
                    elif st == "deadminned":
                        deadmin_admins += v

        if not t.initialized:
            t.last_round = cur_round
            t.goob_last_round_num = cur_round
            t.goob_last_round_id = round_id
            t.goob_last_run_level = run_level
            t.goob_last_round_len = round_len
            t.goob_map = map_name
            t.goob_preset = preset
            t.goob_antags = antags
            t.goob_roundstart_antags = roundstart_antags
            t.goob_events = events
            t.goob_active_ghosts = ghosts
            t.goob_story = story
            t.goob_beat = beat
            t.last_active_admins = active_admins
            t.last_deadminned_admins = deadmin_admins
            for r in antags:
                t.rule_spawn_times[r] = "00:00"
            for r in GOOB_PRESETS.get(preset, {}).get("rules", []):
                t.rule_spawn_times[r] = "00:00"
            if beat == "Denouement":
                t.goob_evac_called = True
                t.goob_evac_docked = True
                t.goob_evac_call_time = round_len
            t.initialized = True
            return

        # -------------------------------------------------------------
        # 0. EVACUATION SHUTTLE TRACKING (Denouement Beat)
        # ONLY during active game (run_level == 1)
        # -------------------------------------------------------------
        if run_level == 1 and beat == "Denouement":
            if not t.goob_evac_called and round_len > 30:
                t.goob_evac_called = True
                t.goob_evac_docked = False
                t.goob_evac_call_time = round_len
                t.goob_evac_msg = await self.notify_channel(
                    channel,
                    f"**[{tag}] Вызван эвакуационный шаттл. Расчетное время прибытия: 10 минут.**",
                    is_critical=True
                )
            elif t.goob_evac_called and not t.goob_evac_docked and t.goob_evac_call_time > 0 and (round_len - t.goob_evac_call_time >= 600):
                t.goob_evac_docked = True
                if t.goob_evac_msg:
                    try:
                        await t.goob_evac_msg.delete()
                    except Exception:
                        pass
                    t.goob_evac_msg = None
                t.goob_evac_msg = await self.notify_channel(
                    channel,
                    f"**[{tag}] Эвакуационный шаттл пристыковался к станции. До отлета 3 минуты.**",
                    is_critical=True
                )
        elif t.goob_evac_called and beat != "Denouement" and run_level == 1:
            t.goob_evac_called = False
            t.goob_evac_docked = False
            t.goob_evac_call_time = 0
            if t.goob_evac_msg:
                try:
                    await t.goob_evac_msg.delete()
                except Exception:
                    pass
                t.goob_evac_msg = None
            await self.notify_channel(
                channel,
                f"**[{tag}] Отзыв эвакуации: вызов эвакуационного шаттла отменен.**",
                is_critical=True
            )

        # -------------------------------------------------------------
        # 1. ROUND END DETECTION
        # -------------------------------------------------------------
        is_round_ended = False
        if t.goob_last_run_level == 1 and run_level in (0, 2) and not t.goob_round_ended:
            is_round_ended = True
        elif cur_round is not None and t.goob_last_round_num is not None and cur_round != t.goob_last_round_num and not t.goob_round_ended and (t.goob_last_round_len > 120 or round_len > 120):
            is_round_ended = True
        elif round_id is not None and t.goob_last_round_id is not None and round_id != t.goob_last_round_id and not t.goob_round_ended and t.goob_last_round_len > 120:
            is_round_ended = True

        if is_round_ended:
            t.goob_round_ended = True
            t.goob_antags_lost.clear()
            t.rule_spawn_times.clear()
            t.goob_midround_rules.clear()
            t.goob_gamerules_announced.clear()
            t.goob_physical_spawns_announced.clear()
            t.goob_evac_call_time = 0
            # Do NOT reset evac flags here; they are reset only on is_round_started so evac doesn't re-trigger in post-round
            end_num = t.goob_last_round_num or cur_round
            end_id = t.goob_last_round_id or round_id
            end_len = t.goob_last_round_len or round_len
            m, s = divmod(end_len, 60)
            len_str = f"{m} мин {s:02d} сек"

            await self.notify_channel(
                channel, 
                f"**[{tag}] Раунд #{end_id or end_num} завершен. Длительность: {len_str} | Карта: {t.goob_map or map_name}**",
                is_critical=True
            )

        # -------------------------------------------------------------
        # 2. LOBBY & ROUND START DETECTION
        # -------------------------------------------------------------
        if run_level == 0 and t.goob_last_run_level != 0:
            if not t.goob_lobby_purged:
                t.goob_lobby_purged = True
                await self.purge_round_messages(t, channel)
            await self.notify_channel(
                channel,
                f"**[{tag}] Сервер перезапустился в лобби.**",
                is_critical=True
            )

        is_round_started = False
        if t.goob_last_run_level in (0, 2) and run_level == 1:
            is_round_started = True
        elif cur_round is not None and t.goob_last_round_num is not None and cur_round != t.goob_last_round_num and run_level == 1:
            is_round_started = True
        elif round_id is not None and t.goob_last_round_id is not None and round_id != t.goob_last_round_id and run_level == 1:
            is_round_started = True
        elif round_len < 40 and t.goob_last_round_len > 120 and run_level == 1:
            is_round_started = True

        if is_round_started:
            t.goob_round_ended = False
            t.goob_evac_called = False
            t.goob_evac_docked = False
            t.goob_evac_call_time = 0
            t.goob_evac_msg = None
            t.goob_lobby_purged = False
            t.last_round = cur_round
            t.goob_last_round_num = cur_round
            t.goob_last_round_id = round_id
            t.goob_last_run_level = run_level
            t.goob_last_round_len = round_len
            t.goob_map = map_name
            t.goob_preset = preset
            t.goob_antags = antags
            t.goob_roundstart_antags = roundstart_antags
            t.goob_events = events
            t.goob_story = story
            t.goob_beat = beat
            t.goob_antags_lost.clear()
            t.rule_spawn_times.clear()
            t.goob_midround_rules.clear()
            t.goob_gamerules_announced.clear()
            t.goob_physical_spawns_announced.clear()

            # Clean all old round messages so channel starts at 0 for new round
            await self.purge_round_messages(t, channel)

            preset_info_init = GOOB_PRESETS.get(preset, {})
            for r in preset_info_init.get("rules", [preset, "GameDirectorRulesTable"]):
                t.rule_spawn_times[r] = "00:00"
            for rs in roundstart_antags:
                mapped = "CalmTraitor" if rs == "Traitor" else ("Calmops" if rs == "Nukeops" else ("CalmLing" if rs == "Changeling" else f"AntagSelection:{rs}"))
                t.rule_spawn_times[mapped] = "00:00"
            if story:
                t.rule_spawn_times[f"Story:{story}"] = "00:00"
            if beat:
                t.rule_spawn_times[f"StoryBeat:{beat}"] = "00:00"

            preset_info = GOOB_PRESETS.get(preset, {})
            preset_title = preset_info.get("title", preset)

            await self.notify_channel(
                channel, 
                f"**[{tag}] Раунд #{round_id or cur_round} начался. Режим: {preset_title} | Карта: {map_name} | Онлайн: {players}**",
                is_critical=True
            )

        t.last_round = cur_round
        t.goob_last_round_num = cur_round
        t.goob_last_round_id = round_id
        t.goob_last_run_level = run_level
        t.goob_last_round_len = round_len
        t.goob_map = map_name
        t.goob_preset = preset
        t.goob_roundstart_antags = roundstart_antags

        # -------------------------------------------------------------
        # 3. Antagonists Delta (Major Antagonists Critical Alerts)
        # ONLY IN ACTIVE GAME (run_level == 1)
        # -------------------------------------------------------------
        if run_level == 1:
            total_antags = sum(antags.values())
            all_rules = set(antags.keys()) | set(t.goob_antags.keys())
            for rule in all_rules:
                cnt = antags.get(rule, 0)
                prev = t.goob_antags.get(rule, 0)
                if cnt > prev and round_len > 30:
                    diff = cnt - prev
                    role_name = get_antag_display(rule)
                    if rule in ROUNDSTART_ANTAG_RULES:
                        t.rule_spawn_times[rule] = "00:00"
                    elif rule not in t.rule_spawn_times:
                        m_t, s_t = divmod(round_len, 60)
                        t.rule_spawn_times[rule] = f"{m_t:02d}:{s_t:02d}"
                    if rule not in t.goob_midround_rules:
                        t.goob_midround_rules.append(rule)

                    if is_major_antag(rule):
                        # If gamerule wasn't announced yet, announce it
                        if rule not in t.goob_gamerules_announced:
                            t.goob_gamerules_announced.add(rule)
                            await self.notify_channel(
                                channel,
                                f"**[{tag}] Gamerule: {role_name}.**",
                                is_critical=True
                            )
                        # If lottery ghost role, announce physical spawn when player takes body
                        if rule in LOTTERY_ANTAG_RULES or "Spawn" in rule or "Pirate" in rule or "Abductor" in rule:
                            if rule not in t.goob_physical_spawns_announced:
                                t.goob_physical_spawns_announced.add(rule)
                                verb = "Заспавнились" if ("Pirate" in rule or "Nuke" in rule or cnt > 1) else "Заспавнился"
                                await self.notify_channel(
                                    channel,
                                    f"**[{tag}] {verb}: {role_name}.**",
                                    is_critical=True
                                )
                elif cnt < prev and round_len > 30:
                    diff = prev - cnt
                    t.goob_antags_lost[rule] = t.goob_antags_lost.get(rule, 0) + diff
                    role_name = get_antag_display(rule)
                    if cnt == 0 and is_major_antag(rule):
                        t.goob_gamerules_announced.discard(rule)
                        t.goob_physical_spawns_announced.discard(rule)
                        await self.notify_channel(
                            channel,
                            f"**[{tag}] Крупный антагонист нейтрализован: {role_name} (убит / вышел).**",
                            is_critical=True
                        )
            t.goob_antags = antags

            # 4. Game Director Events Delta
            for ev_raw, cnt in events.items():
                prev = t.goob_events.get(ev_raw, 0)
                if cnt > prev and round_len > 30:
                    diff = cnt - prev
                    ev_clean = ev_raw.split(" (")[0]
                    antag_rule = EVENT_TO_ANTAG_RULE.get(ev_clean)
                    if antag_rule or is_major_antag(ev_clean):
                        target_rule = antag_rule or ev_clean
                        if target_rule not in t.goob_gamerules_announced:
                            t.goob_gamerules_announced.add(target_rule)
                            role_disp = get_antag_display(target_rule)
                            await self.notify_channel(
                                channel,
                                f"**[{tag}] Gamerule: {role_disp}.**",
                                is_critical=True
                            )
                    elif ev_clean in DISASTER_EVENTS:
                        disaster_title = DISASTER_EVENTS[ev_clean]
                        await self.notify_channel(channel, f"**[{tag}]** Катастрофа: **{disaster_title}** ({ev_clean}).")
            t.goob_events = events

            # 5. Story Beat change
            if beat and t.goob_beat and beat != t.goob_beat:
                story_title = GOOB_STORIES.get(story, story)
                beat_title = GOOB_BEATS.get(beat, beat)
                await self.notify_channel(channel, f"**[{tag}]** Game Director сменил фазу сюжета: **{story_title}** -> **{beat_title}**!")
            t.goob_beat = beat
            t.goob_story = story
        else:
            # Server in lobby or post-round: clear tracking so no bogus alerts fire
            t.goob_antags.clear()
            t.goob_antags_lost.clear()
            t.goob_events.clear()
            t.goob_beat = None
            t.goob_story = None
            t.goob_gamerules_announced.clear()
            t.goob_physical_spawns_announced.clear()

        # -------------------------------------------------------------
        # 6. Admins
        # -------------------------------------------------------------
        if t.last_active_admins is not None and run_level == 1:
            if active_admins == 0 and t.last_active_admins > 0:
                await self.notify_channel(channel, f"**[{tag}]** Внимание: на сервере не осталось активных админов (0 активных, {deadmin_admins} деадмин). Надзор отсутствует.")
            elif active_admins > 0 and t.last_active_admins == 0:
                await self.notify_channel(channel, f"**[{tag}]** На сервере появился активный админ (активных: {active_admins}). Возобновлен надзор.")
        t.last_active_admins = active_admins
        t.last_deadminned_admins = deadmin_admins

        # Live Dashboard Update (Goob Station)
        srv_title = tag.replace('-', ' ')
        if run_level == 0:
            len_str = "0 мин 00 сек"
        else:
            m, s = divmod(round_len, 60)
            len_str = f"{m} мин {s:02d} сек" if round_len > 0 else "0 мин 00 сек"

        if round_id and str(round_id) != "None":
            round_id_str = f"#{round_id}"
        elif cur_round:
            round_id_str = f"#{cur_round}"
        else:
            round_id_str = "?"

        if run_level == 1:
            status_label = "В игре"
            color = 0x2ecc71
        elif run_level == 0:
            status_label = "В лобби"
            color = 0xf1c40f
        else:
            status_label = "Раунд завершен"
            color = 0xe74c3c

        preset_info = GOOB_PRESETS.get(preset, {})
        preset_title = preset_info.get("title", preset)
        story_title = GOOB_STORIES.get(story, story)
        beat_title = GOOB_BEATS.get(beat, beat)

        admins_str = f"{active_admins} активных / {deadmin_admins} скрытых"
        ghosts_str = str(ghosts)
        total_antags = sum(antags.values()) if run_level == 1 else 0
        if run_level == 0:
            antags_total_str = "0"
            antags_roster_str = "• Отсутствуют (0)"
        else:
            antags_total_str = f"Всего: **{total_antags}**"
            has_lost = any(v > 0 for v in t.goob_antags_lost.values())
            if total_antags > 0 or has_lost:
                antags_roster_str = format_goob_roster(antags, t.goob_antags_lost, run_level)
            else:
                antags_roster_str = "• Отсутствуют (0)"

        raw_rules = []
        base_rules = list(preset_info.get("rules", [preset, "GameDirectorRulesTable"]))
        for r in base_rules:
            if r not in t.rule_spawn_times:
                t.rule_spawn_times[r] = "00:00"
            if r not in raw_rules:
                raw_rules.append(r)

        if run_level == 1:
            if roundstart_antags:
                for rs in roundstart_antags:
                    # If real antags are already active on station, don't show the dormant Calm placeholder
                    if rs in antags and (antags[rs] > 0 or t.goob_antags_lost.get(rs, 0) > 0):
                        continue
                    mapped = "CalmTraitor" if rs == "Traitor" else ("Calmops" if rs == "Nukeops" else ("CalmLing" if rs == "Changeling" else f"AntagSelection:{rs}"))
                    if mapped not in t.rule_spawn_times:
                        t.rule_spawn_times[mapped] = "00:00"
                    if mapped not in raw_rules:
                        raw_rules.append(mapped)
            for mr in t.goob_midround_rules:
                if mr not in raw_rules:
                    raw_rules.append(mr)
            for rule_key in antags:
                if (antags[rule_key] > 0 or t.goob_antags_lost.get(rule_key, 0) > 0):
                    # Clean up quiet/calm placeholder if the real antag is now active
                    calm_key = "CalmTraitor" if rule_key == "Traitor" else ("Calmops" if rule_key == "Nukeops" else ("CalmLing" if rule_key == "Changeling" else None))
                    if calm_key and calm_key in raw_rules:
                        raw_rules.remove(calm_key)
                    if rule_key not in raw_rules:
                        if rule_key in ROUNDSTART_ANTAG_RULES:
                            t.rule_spawn_times[rule_key] = "00:00"
                        elif rule_key not in t.rule_spawn_times:
                            m_t, s_t = divmod(round_len, 60)
                            t.rule_spawn_times[rule_key] = f"{m_t:02d}:{s_t:02d}" if round_len > 30 else "00:00"
                        raw_rules.append(rule_key)
            if story:
                sk = f"Story:{story}"
                if sk not in t.rule_spawn_times:
                    t.rule_spawn_times[sk] = "00:00"
                if sk not in raw_rules:
                    raw_rules.append(sk)
            if beat:
                bk = f"StoryBeat:{beat}"
                if bk not in t.rule_spawn_times:
                    m_t, s_t = divmod(round_len, 60)
                    t.rule_spawn_times[bk] = f"{m_t:02d}:{s_t:02d}" if round_len > 30 else "00:00"
                if bk not in raw_rules:
                    raw_rules.append(bk)
        rules_list = [format_rule_entry(r, t.rule_spawn_times.get(r, "00:00")) for r in raw_rules]

        if run_level == 0:
            footer_text = "Ожидание старта раунда"
        elif story and beat:
            footer_text = f"Сюжет Game Director: {story_title} ({beat_title})"
        elif story:
            footer_text = f"Сюжет Game Director: {story_title}"
        else:
            footer_text = ""

        embed = make_dashboard_embed(
            title=srv_title,
            status_label=status_label,
            color=color,
            players=str(players),
            round_len_str=len_str,
            round_id_str=round_id_str,
            map_name=map_name,
            preset_title=preset_title,
            admins_str=admins_str,
            ghosts_str=ghosts_str,
            antags_total_str=antags_total_str,
            antags_roster_str=antags_roster_str,
            rules_list=rules_list,
            footer_text=footer_text
        )
        await self.update_dashboard(t, channel, embed)

    async def poll_corvaxgoob_server(self, t: ServerTracker, instance: str = "void"):
        channel = await self.get_tracker_channel(t)
        if not channel:
            return

        tag = f"CORVAXGOOB-{instance.upper()}"
        status_url = f"https://game1.station14.ru/{instance}/server/status"
        status_data = {}
        try:
            async with self.session.get(status_url, timeout=aiohttp.ClientTimeout(total=4), ssl=False) as resp:
                if resp.status == 200:
                    status_data = await resp.json()
        except Exception:
            pass

        if not status_data:
            return

        run_level = status_data.get("run_level", 1)
        round_id = status_data.get("round_id")
        map_name = status_data.get("map", "Station")
        preset = status_data.get("preset", "Секрет")
        players = status_data.get("players", "?")
        soft_max = status_data.get("soft_max_players", 90)
        round_start = status_data.get("round_start_time")

        round_len = 0
        if round_start:
            try:
                st = round_start.replace("Z", "+00:00")
                start_dt = datetime.fromisoformat(st)
                now_dt = datetime.now(timezone.utc)
                round_len = max(0, int((now_dt - start_dt).total_seconds()))
            except Exception:
                pass

        m, s = divmod(round_len, 60)
        len_str = f"{m} мин {s:02d} сек" if round_len > 0 else "0 мин 00 сек"

        inst_label = "ВОЙД" if instance == "void" else instance.upper()
        srv_title = f"CORVAXGOOB: {inst_label}"
        status_label = "В игре" if run_level != 0 else "В лобби"
        color = 0x2ecc71 if run_level != 0 else 0x95a5a6
        players_display = f"{players}/{soft_max}" if soft_max else str(players)
        round_id_str = f"#{round_id}" if round_id else "?"

        if not t.initialized:
            t.last_round = round_id
            t.cg_last_round_id = round_id
            t.cg_last_run_level = run_level
            t.cg_map = map_name
            t.cg_preset = preset
            t.initialized = True

            embed = make_dashboard_embed(
                title=srv_title,
                status_label=status_label,
                color=color,
                players=players_display,
                round_len_str=len_str,
                round_id_str=round_id_str,
                map_name=map_name,
                preset_title=preset,
                admins_str="Метрики Prometheus закрыты хостом",
                ghosts_str="Закрыты хостом",
                antags_total_str="Закрыты хостом",
                antags_roster_str="• Съем антагов недоступен (порт метрик закрыт хостом)",
                rules_list=[preset, "BasicRoundstartVariation"],
                footer_text=f"Онлайн: {players} игроков"
            )
            await self.update_dashboard(t, channel, embed)
            return

        # 1. ROUND END DETECTION
        is_round_ended = False
        if t.cg_last_run_level == 1 and run_level in (0, 2) and not t.cg_round_ended:
            is_round_ended = True
        elif round_id is not None and t.cg_last_round_id is not None and round_id != t.cg_last_round_id and not t.cg_round_ended:
            is_round_ended = True

        if is_round_ended:
            t.cg_round_ended = True
            await self.notify_channel(
                channel,
                f"**[{tag}] Раунд #{t.cg_last_round_id or round_id} завершен. Длительность: {len_str} | Карта: {t.cg_map or map_name}**",
                is_critical=True
            )

        # 2. LOBBY DETECTION & CLEANUP
        if run_level == 0 and t.cg_last_run_level != 0:
            await self.notify_channel(
                channel,
                f"**[{tag}] Сервер перезапустился в лобби.**",
                is_critical=True
            )
            if not t.cg_lobby_purged:
                t.cg_lobby_purged = True
                await self.purge_round_messages(t, channel)

        # 3. NEW ROUND START DETECTION
        is_round_started = False
        if t.cg_last_run_level in (0, 2) and run_level == 1:
            is_round_started = True
        elif round_id is not None and t.cg_last_round_id is not None and round_id != t.cg_last_round_id and run_level == 1:
            is_round_started = True

        if is_round_started:
            t.cg_round_ended = False
            t.cg_lobby_purged = False
            await self.purge_round_messages(t, channel)

            await self.notify_channel(
                channel,
                f"**[{tag}] Раунд #{round_id} начался. Режим: {preset} | Карта: {map_name} | Онлайн: {players}/{soft_max}**",
                is_critical=True
            )

        t.last_round = round_id
        t.cg_last_round_id = round_id
        t.cg_last_run_level = run_level
        t.cg_map = map_name
        t.cg_preset = preset

        # Live Dashboard Update
        embed = make_dashboard_embed(
            title=srv_title,
            status_label=status_label,
            color=color,
            players=players_display,
            round_len_str=len_str,
            round_id_str=round_id_str,
            map_name=map_name,
            preset_title=preset,
            admins_str="Метрики Prometheus закрыты хостом",
            ghosts_str="Закрыты хостом",
            antags_total_str="Закрыты хостом",
            antags_roster_str="• Съем антагов недоступен (порт метрик закрыт хостом)",
            rules_list=[preset, "BasicRoundstartVariation"],
            footer_text=f"Онлайн: {players} игроков"
        )
        await self.update_dashboard(t, channel, embed)

    async def poll_vp_server(self, t: ServerTracker):
        channel = await self.get_tracker_channel(t)
        if not channel:
            return

        status_data = {}
        try:
            async with self.session.get("http://193.164.18.155:1212/status", timeout=4) as resp:
                if resp.status == 200:
                    status_data = await resp.json()
        except Exception:
            pass

        prom_url = "http://193.164.18.155:9090"
        cur_round = None
        round_len = 0
        players = status_data.get("players", "?")

        try:
            r_round = await prom_query(prom_url, self.session, "ss14_round_number")
            if r_round:
                cur_round = int(r_round[0]["value"][1])
        except Exception:
            pass

        try:
            r_len = await prom_query(prom_url, self.session, "ss14_round_length")
            if r_len and r_len[0]["value"][1]:
                round_len = int(float(r_len[0]["value"][1]))
        except Exception:
            pass

        try:
            r_ply = await prom_query(prom_url, self.session, "robust_player_count")
            if r_ply:
                players = r_ply[0]["value"][1]
        except Exception:
            pass

        active_admins = 0
        deadmin_admins = 0
        try:
            admin_data = await prom_query(prom_url, self.session, "ss14_adminmanager_admins_online_count")
            for a in admin_data:
                st = a.get("metric", {}).get("state")
                v = int(float(a["value"][1]))
                if st == "active":
                    active_admins += v
                elif st == "deadminned":
                    deadmin_admins += v
        except Exception:
            pass

        run_level = status_data.get("run_level", 1)
        round_id = status_data.get("round_id", cur_round)
        map_name = status_data.get("map", "Cyberiada")
        preset = status_data.get("preset", "Extended")

        if not t.initialized:
            t.last_round = cur_round
            t.vp_last_round_num = cur_round
            t.vp_last_round_id = round_id
            t.vp_last_run_level = run_level
            t.vp_last_round_len = round_len
            t.vp_map = map_name
            t.vp_preset = preset
            t.last_active_admins = active_admins
            t.last_deadminned_admins = deadmin_admins
            t.initialized = True
            return

        # 1. Round End
        is_round_ended = False
        if t.vp_last_run_level == 1 and run_level in (0, 2) and not t.vp_round_ended:
            is_round_ended = True
        elif cur_round is not None and t.vp_last_round_num is not None and cur_round != t.vp_last_round_num and not t.vp_round_ended and (t.vp_last_round_len > 120 or round_len > 120):
            is_round_ended = True
        elif round_id is not None and t.vp_last_round_id is not None and round_id != t.vp_last_round_id and not t.vp_round_ended and t.vp_last_round_len > 120:
            is_round_ended = True

        if is_round_ended:
            t.vp_round_ended = True
            end_num = t.vp_last_round_num or cur_round
            end_id = t.vp_last_round_id or round_id
            end_len = t.vp_last_round_len or round_len
            m, s = divmod(end_len, 60)
            len_str = f"{m} мин {s:02d} сек"

            preset_info = ADT_PRESETS.get(t.vp_preset or preset, {})
            preset_title = preset_info.get("title", t.vp_preset or preset)
            antags_summary = preset_info.get("antags", "По активному режиму")

            await self.notify_channel(channel, 
                f"**[ВРЕМЯ ПРИКЛЮЧЕНИЙ] Раунд #{end_id or end_num} завершен.**\n"
                f"• Длительность: **{len_str}**\n"
                f"• Карта: **{t.vp_map or map_name}** | Режим: **{preset_title}**\n"
                f"• Антагонисты раунда: {antags_summary}\n"
                f"• Онлайн под конец: **{players}** игроков"
            )

        # 2. Round Start
        is_round_started = False
        if t.vp_last_run_level in (0, 2) and run_level == 1:
            is_round_started = True
        elif cur_round is not None and t.vp_last_round_num is not None and cur_round != t.vp_last_round_num and run_level == 1:
            is_round_started = True
        elif round_id is not None and t.vp_last_round_id is not None and round_id != t.vp_last_round_id and run_level == 1:
            is_round_started = True
        elif round_len < 40 and t.vp_last_round_len > 120 and run_level == 1:
            is_round_started = True

        if is_round_started:
            t.vp_round_ended = False
            t.last_round = cur_round
            t.vp_last_round_num = cur_round
            t.vp_last_round_id = round_id
            t.vp_last_run_level = run_level
            t.vp_last_round_len = round_len
            t.vp_map = map_name
            t.vp_preset = preset

            preset_info = ADT_PRESETS.get(preset, {})
            preset_title = preset_info.get("title", preset)
            antags_summary = preset_info.get("antags", "По активному режиму")
            rules_text = format_adt_rules_stack(preset)

            await self.notify_channel(channel, 
                f"**[ВРЕМЯ ПРИКЛЮЧЕНИЙ] Раунд #{round_id or cur_round} начался.**\n"
                f"• Карта: **{map_name}** | Онлайн: **{players}** игроков\n"
                f"• Режим: **{preset_title}**\n"
                f"• Состав антагонистов: {antags_summary}\n\n"
                f"Активные геймрулы:\n{rules_text}"
            )
            return

        t.last_round = cur_round
        t.vp_last_round_num = cur_round
        t.vp_last_round_id = round_id
        t.vp_last_run_level = run_level
        t.vp_last_round_len = round_len
        t.vp_map = map_name
        t.vp_preset = preset

        # 3. Admins
        if t.last_active_admins is not None:
            if active_admins == 0 and t.last_active_admins > 0:
                await self.notify_channel(channel, f"**[ВРЕМЯ ПРИКЛЮЧЕНИЙ]** Внимание: на сервере не осталось активных админов (0 активных, {deadmin_admins} деадмин). Надзор отсутствует.")
            elif active_admins > 0 and t.last_active_admins == 0:
                await self.notify_channel(channel, f"**[ВРЕМЯ ПРИКЛЮЧЕНИЙ]** На сервере появился активный админ (активных: {active_admins}). Возобновлен надзор.")
        t.last_active_admins = active_admins
        t.last_deadminned_admins = deadmin_admins

        # Live Dashboard Update (Adventure Time / VP)
        srv_title = "ВРЕМЯ ПРИКЛЮЧЕНИЙ"
        m, s = divmod(round_len, 60)
        len_str = f"{m} мин {s:02d} сек" if round_len > 0 else "0 мин 00 сек"
        if round_id and str(round_id) != "None":
            round_id_str = f"#{round_id}"
        elif cur_round:
            round_id_str = f"#{cur_round}"
        else:
            round_id_str = "?"

        status_label = "В игре" if run_level == 1 else "В лобби"
        color = 0x2ecc71 if run_level == 1 else 0xf1c40f

        preset_info = ADT_PRESETS.get(preset, {})
        preset_title = preset_info.get("title", preset)
        antags_desc = preset_info.get("antags", "По активному режиму")

        admins_str = f"{active_admins} активных / {deadmin_admins} скрытых"
        ghosts_str = "Проверяйте F7 в игре"
        antags_total_str = "По активному режиму"
        antags_roster_str = f"• {antags_desc}"

        rules_list = list(preset_info.get("rules", [preset, "BasicRoundstartVariation"]))
        footer_text = "Ядро: ADT"

        embed = make_dashboard_embed(
            title=srv_title,
            status_label=status_label,
            color=color,
            players=str(players),
            round_len_str=len_str,
            round_id_str=round_id_str,
            map_name=map_name,
            preset_title=preset_title,
            admins_str=admins_str,
            ghosts_str=ghosts_str,
            antags_total_str=antags_total_str,
            antags_roster_str=antags_roster_str,
            rules_list=rules_list,
            footer_text=footer_text
        )
        await self.update_dashboard(t, channel, embed)

    @radar_loop.before_loop
    async def before_radar(self):
        await self.wait_until_ready()


bot = TelemetryBot()


def resolve_server(interaction: discord.Interaction, passed_server: str = None) -> str:
    if passed_server:
        s = passed_server.lower()
        if s in ("sigma", "goob-sigma"):
            return "goob-sigma"
        if s in ("basileus", "baselius", "omega", "goob-basileus", "goob-baselius"):
            return "goob-basileus"
        if s in ("adventure", "adventure_time"):
            return "vp"
        if s in ("persey", "perseus", "fish", "fish-station", "fish_station"):
            return "perseus"
        if s in ("void", "corvaxgoob", "corvaxgoob-void", "corvaxgoob_void"):
            return "corvaxgoob-void"
        if s in ("lumen", "corvaxgoob-lumen", "corvaxgoob_lumen"):
            return "corvaxgoob-lumen"
        if s in ("spectral", "venus", "perseus", "invicta", "astra", "fenix", "fire", "vp", "corvaxgoob-void"):
            return s
    for s_name, ch_id in bot.cfg.get("server_channels", {}).items():
        if interaction.channel_id == ch_id:
            return s_name
    return bot.cfg.get("default_server", "spectral")


@bot.tree.command(name="setchannel", description="Привязать текущий канал к серверу для оповещений")
@app_commands.describe(server="Сервер для привязки (spectral, venus, goob-sigma, goob-basileus, vp)")
async def cmd_setchannel(interaction: discord.Interaction, server: str = "spectral"):
    server = server.lower()
    if server in ("void", "corvaxgoob", "corvaxgoob-void", "corvaxgoob_void"):
        server = "corvaxgoob-void"
    elif server in ("lumen", "corvaxgoob-lumen", "corvaxgoob_lumen"):
        server = "corvaxgoob-lumen" 
    if "server_channels" not in bot.cfg:
        bot.cfg["server_channels"] = {}
    bot.cfg["server_channels"][server] = interaction.channel_id

    if server in bot.trackers:
        bot.trackers[server].channel_id = interaction.channel_id
        bot.trackers[server].cached_channel = interaction.channel
    else:
        bot.trackers[server] = ServerTracker(server, interaction.channel_id)
        bot.trackers[server].cached_channel = interaction.channel

    save_config(bot.cfg)
    await interaction.response.send_message(
        f"Канал <#{interaction.channel_id}> привязан к серверу **{server.upper()}**.",
        ephemeral=True
    )


@bot.tree.command(name="clearchat", description="Очистить старые сообщения бота в канале и пересоздать живую табличку")
async def cmd_clearchat(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    deleted = 0
    try:
        async for m in interaction.channel.history(limit=50):
            if m.author.id == bot.user.id:
                try:
                    await m.delete()
                    deleted += 1
                except Exception:
                    pass
    except Exception:
        pass

    server = resolve_server(interaction)
    if server in bot.trackers:
        bot.trackers[server].dashboard_msg = None
        bot.trackers[server].last_dashboard_fingerprint = ""
    if "dashboard_messages" in bot.cfg and server in bot.cfg["dashboard_messages"]:
        del bot.cfg["dashboard_messages"][server]
        save_config(bot.cfg)

    await interaction.followup.send(
        f"Очищено {deleted} сообщений бота. Живая табличка обновится в канале.",
        ephemeral=True
    )


@bot.tree.command(name="admins", description="Количество активных и скрытых (деадмин) администраторов")
@app_commands.describe(server="Сервер (spectral, venus, goob-sigma, goob-basileus, vp, perseus, invicta)")
async def cmd_admins(interaction: discord.Interaction, server: str = None):
    await interaction.response.defer()
    server = resolve_server(interaction, server)

    if server in ("vp", "adventure", "adventure_time"):
        prom_url = "http://193.164.18.155:9090"
        admin_data = await prom_query(prom_url, bot.session, "ss14_adminmanager_admins_online_count")
        active = 0
        deadminned = 0
        for a in admin_data:
            st = a.get("metric", {}).get("state")
            v = int(float(a["value"][1]))
            if st == "active":
                active += v
            elif st == "deadminned":
                deadminned += v
        status_note = "Надзор активен." if active > 0 else "Активный надзор отсутствует (все деадмин/оффлайн)."
        msg = (
            f"Обстановка по администрации на **ВРЕМЯ ПРИКЛЮЧЕНИЙ**:\n"
            f"- Активных админов (с панелью): **{active}**\n"
            f"- Скрытых админов (деадмин / играют): **{deadminned}**\n"
            f"- Всего на сервере: **{active + deadminned}**\n\n"
            f"Статус: {status_note}"
        )
        await interaction.followup.send(msg)
        return

    if server.startswith("goob"):
        status_url, metrics_url, tag = get_goob_target(server)
        raw_metrics = ""
        try:
            async with bot.session.get(metrics_url, timeout=6, ssl=False) as resp:
                if resp.status == 200:
                    raw_metrics = await resp.text()
        except Exception:
            pass
        active = 0
        deadminned = 0
        for line in raw_metrics.splitlines():
            if line.startswith("ss14_adminmanager_admins_online_count{"):
                m = re.match(r'ss14_adminmanager_admins_online_count\{rank="[^"]*",state="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    st = m.group(1)
                    v = int(float(m.group(2)))
                    if st == "active":
                        active += v
                    elif st == "deadminned":
                        deadminned += v
        status_note = "Надзор активен." if active > 0 else "Активный надзор отсутствует (все деадмин/оффлайн)."
        msg = (
            f"Обстановка по администрации на **{tag}**:\n"
            f"- Активных админов (с панелью): **{active}**\n"
            f"- Скрытых админов (деадмин / играют): **{deadminned}**\n"
            f"- Всего на сервере: **{active + deadminned}**\n\n"
            f"Статус: {status_note}"
        )
        await interaction.followup.send(msg)
        return

    prom_url = bot.cfg.get("prometheus_url", "http://185.97.255.6:9090")
    admin_data = await prom_query(prom_url, bot.session, f"ss14_adminmanager_admins_online_count{{job='{server}'}}")
    active = 0
    deadminned = 0
    afk = 0
    for a in admin_data:
        st = a.get("metric", {}).get("state")
        v = int(a["value"][1])
        if st == "active":
            active = v
        elif st == "deadminned":
            deadminned = v
        elif st == "afk":
            afk = v

    total = active + deadminned + afk
    status_note = "Надзор активен." if active > 0 else "Активный надзор отсутствует (все деадмин/оффлайн)."

    msg = (
        f"Обстановка по администрации на **{server.upper()}**:\n"
        f"- Активных админов (с панелью): **{active}**\n"
        f"- Скрытых админов (деадмин / играют): **{deadminned}**\n"
        f"- В АФК: **{afk}**\n"
        f"- Всего на сервере: **{total}**\n\n"
        f"Статус: {status_note}"
    )
    await interaction.followup.send(msg)


@bot.tree.command(name="antags", description="Количество и точный состав живых антагонистов, гост-ролей и геймрулы")
@app_commands.describe(server="Сервер (spectral, venus, goob-sigma, goob-basileus, vp, perseus, invicta)")
async def cmd_antags(interaction: discord.Interaction, server: str = None):
    await interaction.response.defer()
    server = resolve_server(interaction, server)

    if server in ("vp", "adventure", "adventure_time"):
        status_data = {}
        try:
            async with bot.session.get("http://193.164.18.155:1212/status", timeout=4) as resp:
                if resp.status == 200:
                    status_data = await resp.json()
        except Exception:
            pass

        prom_url = "http://193.164.18.155:9090"
        cur_round = "?"
        m_len = 0
        players = status_data.get("players", "?")

        try:
            r_round = await prom_query(prom_url, bot.session, "ss14_round_number")
            if r_round:
                cur_round = r_round[0]["value"][1]
        except Exception:
            pass

        try:
            r_len = await prom_query(prom_url, bot.session, "ss14_round_length")
            if r_len and r_len[0]["value"][1]:
                m_len = int(float(r_len[0]["value"][1]))
        except Exception:
            pass

        m, s = divmod(m_len, 60)
        len_str = f" ({m:02d}:{s:02d})" if m_len > 0 else ""

        map_name = status_data.get("map", "Cyberiada")
        round_id = status_data.get("round_id", cur_round)
        preset = status_data.get("preset", "Extended")

        preset_info = ADT_PRESETS.get(preset, {})
        preset_title = preset_info.get("title", preset)
        antags_desc = preset_info.get("antags", "По активному режиму")
        rules_text = format_adt_rules_stack(preset)

        msg = (
            f"Обстановка по антагонистам на **ВРЕМЯ ПРИКЛЮЧЕНИЙ**:\n"
            f"- Карта: **{map_name}** | Раунд: **#{round_id or cur_round}**{len_str}\n"
            f"- Онлайн: **{players}** игроков\n"
            f"- Режим: **{preset_title}**\n"
            f"- Состав антагонистов: **{antags_desc}**\n\n"
            f"Активные геймрулы:\n{rules_text}"
        )
        await interaction.followup.send(msg)
        return

    if server.startswith("goob"):
        status_url, metrics_url, tag = get_goob_target(server)
        status_data = {}
        try:
            async with bot.session.get(status_url, timeout=5, ssl=False) as resp:
                if resp.status == 200:
                    status_data = await resp.json()
        except Exception:
            pass

        raw_metrics = ""
        try:
            async with bot.session.get(metrics_url, timeout=6, ssl=False) as resp:
                if resp.status == 200:
                    raw_metrics = await resp.text()
        except Exception:
            pass

        cur_round = "?"
        m_rnd = re.search(r"ss14_round_number\s+([0-9]+)", raw_metrics)
        if m_rnd:
            cur_round = m_rnd.group(1)

        m_len = re.search(r"ss14_round_length\s+([0-9.]+)", raw_metrics)
        length_sec = int(float(m_len.group(1))) if m_len else 0
        m, s = divmod(length_sec, 60)
        len_str = f" ({m:02d}:{s:02d})" if length_sec > 0 else ""

        antags = {}
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_antags{"):
                m = re.match(r'game_director_antags\{rule="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    antags[m.group(1)] = int(float(m.group(2)))

        ghosts = 0
        m_g = re.search(r"game_director_active_ghosts\s+([0-9.]+)", raw_metrics)
        if m_g:
            ghosts = int(float(m_g.group(1)))

        story = "?"
        beat = "?"
        m_b = re.search(r'game_director_beat_info\{story_name="([^"]+)",beat_name="([^"]+)"\}\s+1', raw_metrics)
        if m_b:
            story = m_b.group(1)
            beat = m_b.group(2)

        events = []
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_events_run_total{"):
                m = re.match(r'game_director_events_run_total\{event_name="([^"]+)",beat_name="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    events.append(f"{m.group(1)} ({m.group(2)})")

        roundstart_antags = []
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_roundstart_antags_selected_total{"):
                m = re.match(r'game_director_roundstart_antags_selected_total\{antag_name="([^"]+)"\}\s+([0-9.]+)', line)
                if m and float(m.group(2)) > 0:
                    roundstart_antags.append(m.group(1))

        map_name = status_data.get("map", "Unknown")
        round_id = status_data.get("round_id", cur_round)
        players = status_data.get("players", "?")
        preset = status_data.get("preset", "The Guide")

        preset_info = GOOB_PRESETS.get(preset, {})
        preset_title = preset_info.get("title", preset)
        story_title = GOOB_STORIES.get(story, story)
        beat_title = GOOB_BEATS.get(beat, beat)
        rules_text = format_goob_rules_stack(preset, roundstart_antags, story, beat)

        total_antags = sum(antags.values())
        t_tracker = bot.trackers.get(server)
        lost_map = t_tracker.goob_antags_lost if t_tracker else {}
        spawn_times = t_tracker.rule_spawn_times if t_tracker else {}
        mid_rules = t_tracker.goob_midround_rules if t_tracker else []
        has_lost = any(v > 0 for v in lost_map.values())
        antags_list_str = format_goob_roster(antags, lost_map) if (total_antags > 0 or has_lost) else "• Все антагонисты нейтрализованы (0)."
        rules_text = format_goob_rules_stack(preset, roundstart_antags, story, beat, spawn_times, mid_rules)

        ev_str = "\n".join([f"  • `{ev}`" for ev in events]) if events else "  • `(нет)`"

        msg = (
            f"Обстановка по антагонистам на **{tag}**:\n"
            f"- Карта: **{map_name}** | Раунд: **#{round_id or cur_round}**{len_str}\n"
            f"- Онлайн: **{players}** игроков\n"
            f"- Живых антагонистов: **{total_antags}**\n"
            f"{antags_list_str}\n"
            f"- В лобби / призраков (ожидают спавна / роли): **{ghosts}**\n"
            f"- Сценарий Game Director: **{story_title}** (фаза: **{beat_title}**)\n"
            f"- Режим: **{preset_title}**\n\n"
            f"Геймрулы:\n{rules_text}\n\n"
            f"События Game Director в раунде:\n{ev_str}"
        )
        await interaction.followup.send(msg)
        return

    prom_url = bot.cfg.get("prometheus_url", "http://185.97.255.6:9090")

    r_cnt = await prom_query(prom_url, bot.session, f"antags_player_count{{job='{server}'}}")
    count = r_cnt[0]["value"][1] if r_cnt else "0"
    antags_num = int(count)

    r_ghost = await prom_query(prom_url, bot.session, f"ss14_storyteller_available_ghost_roles{{job='{server}'}}")
    ghost_count = r_ghost[0]["value"][1] if r_ghost else "0"

    t = bot.trackers.get(server)
    preset_name = t.current_preset if t else None
    if not preset_name:
        preset_name = await fetch_server_preset(server, bot.session)
    if not preset_name:
        sp_data = await prom_query(prom_url, bot.session, f"secret_preset_selected{{job='{server}'}}")
        if sp_data:
            preset_name = sp_data[-1].get("metric", {}).get("preset", "Traitor")
        else:
            preset_name = "Greenshift"

    preset_title = PRESET_NAMES.get(preset_name, preset_name)
    rules_text = format_rules_stack(preset_name)

    role_desc = PRESET_ANTAG_NAMES.get(preset_name, f"Антагонисты ({preset_name})")
    if antags_num == 0:
        composition_text = "Все антагонисты нейтрализованы или раунд мирный (0 живых)."
    else:
        composition_text = f"**{antags_num}x {role_desc}**"

    active_threats = []
    r_len = await prom_query(prom_url, bot.session, f"ss14_round_length{{job='{server}'}}")
    round_len_s = max(60, int(float(r_len[0]["value"][1]))) if r_len and r_len[0]["value"][1] else 3600

    currencies = await prom_query(
        prom_url,
        bot.session,
        f"increase(ss14_store_currency_spent_total{{job='{server}'}}[{round_len_s}s])"
    )
    for c in currencies:
        curr = c.get("metric", {}).get("currency")
        val = int(round(float(c["value"][1])))
        if curr == "Telecrystal" and val > 0:
            active_threats.append(f"Синдикат (потрачено {val} ТК в аплинке)")
        elif curr == "ChangelingDNA" and val > 0:
            active_threats.append(f"Генокрады ({val} очков ДНК)")
        elif curr == "WizCoin" and val > 0:
            active_threats.append(f"Маги ({val} монет в спеллбуке)")
        elif curr == "StolenEssence" and val > 0:
            active_threats.append(f"Вампиры ({val} украденной крови)")
        elif curr == "SiliconMemory" and val > 0:
            active_threats.append(f"Взломанные киборги ({val} памяти)")

    threats_line = ""
    if active_threats:
        threats_line = "\n- Траты валют за текущий раунд: " + "; ".join(active_threats)

    msg = (
        f"Обстановка по антагонистам на **{server.upper()}**:\n"
        f"- Живых антагонистов: **{antags_num}**\n"
        f"- Текущий состав: {composition_text}{threats_line}\n"
        f"- Доступно в F7 (гост-роли): **{ghost_count}** свободных мест\n"
        f"- Режим раунда: **{preset_title}**\n\n"
        f"Геймрулы:\n{rules_text}"
    )
    await interaction.followup.send(msg)


@bot.tree.command(name="status", description="Краткая сводка по станции: антаги, админы, раунд, геймрулы")
@app_commands.describe(server="Сервер (spectral, venus, goob-sigma, goob-basileus, vp, perseus, invicta)")
async def cmd_status(interaction: discord.Interaction, server: str = None):
    await interaction.response.defer()
    server = resolve_server(interaction, server)

    if server in ("vp", "adventure", "adventure_time"):
        status_data = {}
        try:
            async with bot.session.get("http://193.164.18.155:1212/status", timeout=4) as resp:
                if resp.status == 200:
                    status_data = await resp.json()
        except Exception:
            pass

        prom_url = "http://193.164.18.155:9090"
        cur_round = "?"
        m_len = 0
        players = status_data.get("players", "?")

        try:
            r_round = await prom_query(prom_url, bot.session, "ss14_round_number")
            if r_round:
                cur_round = r_round[0]["value"][1]
        except Exception:
            pass

        try:
            r_len = await prom_query(prom_url, bot.session, "ss14_round_length")
            if r_len and r_len[0]["value"][1]:
                m_len = int(float(r_len[0]["value"][1]))
        except Exception:
            pass

        m, s = divmod(m_len, 60)
        len_str = f" ({m:02d}:{s:02d})" if m_len > 0 else ""

        active_adm = 0
        deadmin_adm = 0
        try:
            admin_data = await prom_query(prom_url, bot.session, "ss14_adminmanager_admins_online_count")
            for a in admin_data:
                st = a.get("metric", {}).get("state")
                v = int(float(a["value"][1]))
                if st == "active":
                    active_adm += v
                elif st == "deadminned":
                    deadmin_adm += v
        except Exception:
            pass

        entities_cnt = "?"
        try:
            r_ent = await prom_query(prom_url, bot.session, "robust_entities_count")
            if r_ent:
                entities_cnt = r_ent[0]["value"][1]
        except Exception:
            pass

        map_name = status_data.get("map", "Cyberiada")
        round_id = status_data.get("round_id", cur_round)
        preset = status_data.get("preset", "Extended")
        panic_bunker = "Активен (вход ограничен)" if status_data.get("panic_bunker") else "Отключен"

        preset_info = ADT_PRESETS.get(preset, {})
        preset_title = preset_info.get("title", preset)
        antags_desc = preset_info.get("antags", "По активному режиму")
        rules_text = format_adt_rules_stack(preset)

        msg = (
            f"Сводка по станции **ВРЕМЯ ПРИКЛЮЧЕНИЙ**:\n"
            f"- Карта: **{map_name}** | Раунд: **#{round_id or cur_round}**{len_str}\n"
            f"- Режим: **{preset_title}**\n"
            f"- Антагонисты: {antags_desc}\n"
            f"- Онлайн: **{players}** игроков\n"
            f"- Администрация: **{active_adm}** активных, **{deadmin_adm}** скрытых\n"
            f"- Активных сущностей (Entities): **{entities_cnt}**\n"
            f"- Паник-бункер: {panic_bunker}\n\n"
            f"Активные геймрулы:\n{rules_text}"
        )
        await interaction.followup.send(msg)
        return

    if server.startswith("goob"):
        status_url, metrics_url, tag = get_goob_target(server)
        status_data = {}
        try:
            async with bot.session.get(status_url, timeout=5, ssl=False) as resp:
                if resp.status == 200:
                    status_data = await resp.json()
        except Exception:
            pass

        raw_metrics = ""
        try:
            async with bot.session.get(metrics_url, timeout=6, ssl=False) as resp:
                if resp.status == 200:
                    raw_metrics = await resp.text()
        except Exception:
            pass

        cur_round = "?"
        m_rnd = re.search(r"ss14_round_number\s+([0-9]+)", raw_metrics)
        if m_rnd:
            cur_round = m_rnd.group(1)

        m_len = re.search(r"ss14_round_length\s+([0-9.]+)", raw_metrics)
        length_sec = int(float(m_len.group(1))) if m_len else 0
        m, s = divmod(length_sec, 60)
        len_str = f" ({m:02d}:{s:02d})" if length_sec > 0 else ""

        antags = {}
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_antags{"):
                m = re.match(r'game_director_antags\{rule="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    antags[m.group(1)] = int(float(m.group(2)))

        ghosts = 0
        m_g = re.search(r"game_director_active_ghosts\s+([0-9.]+)", raw_metrics)
        if m_g:
            ghosts = int(float(m_g.group(1)))

        story = "?"
        beat = "?"
        m_b = re.search(r'game_director_beat_info\{story_name="([^"]+)",beat_name="([^"]+)"\}\s+1', raw_metrics)
        if m_b:
            story = m_b.group(1)
            beat = m_b.group(2)

        active_adm = 0
        deadmin_adm = 0
        for line in raw_metrics.splitlines():
            if line.startswith("ss14_adminmanager_admins_online_count{"):
                m = re.match(r'ss14_adminmanager_admins_online_count\{rank="[^"]*",state="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    st = m.group(1)
                    v = int(float(m.group(2)))
                    if st == "active":
                        active_adm += v
                    elif st == "deadminned":
                        deadmin_adm += v

        dept_staff = {}
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_department_staff{"):
                m = re.match(r'game_director_department_staff\{department="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    dept_staff[m.group(1)] = int(float(m.group(2)))

        roundstart_antags = []
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_roundstart_antags_selected_total{"):
                m = re.match(r'game_director_roundstart_antags_selected_total\{antag_name="([^"]+)"\}\s+([0-9.]+)', line)
                if m and float(m.group(2)) > 0:
                    roundstart_antags.append(m.group(1))

        map_name = status_data.get("map", "Unknown")
        round_id = status_data.get("round_id", cur_round)
        players = status_data.get("players", "?")
        preset = status_data.get("preset", "The Guide")

        preset_info = GOOB_PRESETS.get(preset, {})
        preset_title = preset_info.get("title", preset)
        story_title = GOOB_STORIES.get(story, story)
        beat_title = GOOB_BEATS.get(beat, beat)
        rules_text = format_goob_rules_stack(preset, roundstart_antags, story, beat)

        total_antags = sum(antags.values())
        antag_breakdown = ", ".join([f"{get_antag_display(k)}: {v}" for k, v in antags.items()]) if antags else "0"
        staff_str = ", ".join([f"{k}: {v}" for k, v in sorted(dept_staff.items(), key=lambda x: x[1], reverse=True)]) if dept_staff else "нет данных"

        msg = (
            f"Сводка по станции **{tag}**:\n"
            f"- Карта: **{map_name}** | Раунд: **#{round_id or cur_round}**{len_str}\n"
            f"- Режим: **{preset_title}**\n"
            f"- Сюжет Game Director: **{story_title}** ({beat_title})\n"
            f"- Онлайн: **{players}** игроков | Призраков в лобби: **{ghosts}**\n"
            f"- Живых антагонистов: **{total_antags}** ({antag_breakdown})\n"
            f"- Администрация: **{active_adm}** активных, **{deadmin_adm}** скрытых\n"
            f"- Персонал по отделам: {staff_str}\n\n"
            f"Активные геймрулы:\n{rules_text}"
        )
        await interaction.followup.send(msg)
        return

    prom_url = bot.cfg.get("prometheus_url", "http://185.97.255.6:9090")

    r_round = await prom_query(prom_url, bot.session, f"ss14_round_number{{job='{server}'}}")
    cur_round = r_round[0]["value"][1] if r_round else "?"

    r_len = await prom_query(prom_url, bot.session, f"ss14_round_length{{job='{server}'}}")
    length_sec = int(float(r_len[0]["value"][1])) if r_len and r_len[0]["value"][1] else 0
    m, s = divmod(length_sec, 60)

    r_players = await prom_query(prom_url, bot.session, f"robust_player_count{{job='{server}'}}")
    players = r_players[0]["value"][1] if r_players else "?"

    r_alive = await prom_query(prom_url, bot.session, f"ss14_storyteller_alive_players{{job='{server}'}}")
    alive = r_alive[0]["value"][1] if r_alive else "?"

    r_dead = await prom_query(prom_url, bot.session, f"ss14_storyteller_dead_players{{job='{server}'}}")
    dead = r_dead[0]["value"][1] if r_dead else "?"

    r_antags = await prom_query(prom_url, bot.session, f"antags_player_count{{job='{server}'}}")
    antags_cnt = r_antags[0]["value"][1] if r_antags else "0"

    admin_data = await prom_query(prom_url, bot.session, f"ss14_adminmanager_admins_online_count{{job='{server}'}}")
    active_adm = 0
    deadmin_adm = 0
    for a in admin_data:
        st = a.get("metric", {}).get("state")
        v = int(a["value"][1])
        if st == "active":
            active_adm = v
        elif st == "deadminned":
            deadmin_adm = v

    t = bot.trackers.get(server)
    preset_name = t.current_preset if t else None
    if not preset_name:
        preset_name = await fetch_server_preset(server, bot.session) or "Greenshift"

    preset_title = PRESET_NAMES.get(preset_name, preset_name)
    rules_text = format_rules_stack(preset_name)
    role_desc = PRESET_ANTAG_NAMES.get(preset_name, preset_name)
    antag_status_str = f"**{antags_cnt}** ({role_desc})" if int(antags_cnt) > 0 else "**0**"

    r_roles = await prom_query(prom_url, bot.session, f"ss14_storyteller_available_ghost_roles{{job='{server}'}}")
    ghost_roles = r_roles[0]["value"][1] if r_roles else "0"

    r_alert = await prom_query(prom_url, bot.session, f"ss14_storyteller_alert_level{{job='{server}'}}")
    alert_lvl = int(r_alert[0]["value"][1]) if r_alert else 0
    alert_name = ALERT_MAP.get(alert_lvl, f"Уровень {alert_lvl}")

    r_atmos = await prom_query(prom_url, bot.session, f"ss14_storyteller_atmos_unsafe_ratio{{job='{server}'}}")
    atmos_pct = f"{float(r_atmos[0]['value'][1]) * 100:.1f}%" if r_atmos else "0%"

    r_tesla = await prom_query(prom_url, bot.session, f"ss14_storyteller_tesla_active{{job='{server}'}}")
    tesla = "врублена" if r_tesla and r_tesla[0]["value"][1] == "1" else "выключена"

    len_str = f" ({m:02d}:{s:02d})" if length_sec > 0 else ""
    msg = (
        f"Сводка по станции **{server.upper()}**:\n"
        f"- Раунд: #{cur_round}{len_str}\n"
        f"- Режим: **{preset_title}**\n"
        f"- Живых антагонистов: {antag_status_str}\n"
        f"- Администрация: **{active_adm}** активных, **{deadmin_adm}** скрытых\n"
        f"- Свободных гост-ролей (F7): **{ghost_roles}**\n"
        f"- Онлайн: {players} (живых: {alive}, погибших: {dead})\n"
        f"- Код тревоги: {alert_name}\n"
        f"- Опасная атмосфера: {atmos_pct}\n"
        f"- Тесла: {tesla}\n\n"
        f"Активные геймрулы:\n{rules_text}"
    )
    await interaction.followup.send(msg)


@bot.tree.command(name="director", description="Состояние сторителлера и баланс угроз")
@app_commands.describe(server="Сервер (spectral, venus, perseus, invicta)")
async def cmd_director(interaction: discord.Interaction, server: str = None):
    await interaction.response.defer()
    server = resolve_server(interaction, server)
    prom_url = bot.cfg.get("prometheus_url", "http://185.97.255.6:9090")

    r_pacing = await prom_query(prom_url, bot.session, f"ss14_storyteller_pacing_state{{job='{server}'}}")
    pacing_code = int(r_pacing[0]["value"][1]) if r_pacing else 0
    p_title = PACING_MAP.get(pacing_code, f"Неизвестно ({pacing_code})")

    r_threat = await prom_query(prom_url, bot.session, f"ss14_storyteller_threat_budget{{job='{server}'}}")
    threat = f"{float(r_threat[0]['value'][1]):.1f}" if r_threat else "0"

    r_major = await prom_query(prom_url, bot.session, f"ss14_storyteller_major_threat_budget{{job='{server}'}}")
    major = f"{float(r_major[0]['value'][1]):.1f}" if r_major else "0"

    r_stress = await prom_query(prom_url, bot.session, f"ss14_storyteller_crew_stress{{job='{server}'}}")
    stress = f"{float(r_stress[0]['value'][1]):.1f}" if r_stress else "0"

    msg = (
        f"Сторителлер на **{server.upper()}**:\n"
        f"- Фаза: **{p_title}**\n"
        f"- Бюджет малых угроз: {threat} / 250\n"
        f"- Бюджет мажорных антагов: {major}\n"
        f"- Стресс экипажа: {stress}%"
    )
    await interaction.followup.send(msg)


@bot.tree.command(name="armory", description="Заказы вооружения через Карго")
@app_commands.describe(server="Сервер (spectral, venus, perseus, invicta)")
async def cmd_armory(interaction: discord.Interaction, server: str = None):
    await interaction.response.defer()
    server = resolve_server(interaction, server)
    prom_url = bot.cfg.get("prometheus_url", "http://185.97.255.6:9090")

    r_cargo = await prom_query(prom_url, bot.session, f"ss14_cargo_order_fulfilled_total{{job='{server}', product_id=~'Armory.*|.*Weapon.*|.*Bullet.*|.*Grenade.*'}}")

    if not r_cargo:
        await interaction.followup.send(f"На сервере **{server.upper()}** поставок оружия в карго не зафиксировано.")
        return

    lines = [f"Поставки оружия в Карго на **{server.upper()}**:"]
    for c in r_cargo:
        pid = c.get("metric", {}).get("product_id")
        cnt = c["value"][1]
        lines.append(f"- {pid}: {cnt} шт.")

    await interaction.followup.send("\n".join(lines))


@bot.tree.command(name="ghosts", description="Информация о свободных гост-ролях и активных призраках")
@app_commands.describe(server="Сервер (spectral, venus, goob-sigma, goob-basileus, vp, perseus, invicta)")
async def cmd_ghosts(interaction: discord.Interaction, server: str = None):
    await interaction.response.defer()
    server = resolve_server(interaction, server)

    if server in ("vp", "adventure", "adventure_time"):
        await interaction.followup.send(
            "На сервере **ВРЕМЯ ПРИКЛЮЧЕНИЙ** количество свободных мест в F7 не экспортируется в публичные метрики. Проверяйте меню F7 в игре."
        )
        return

    if server.startswith("goob"):
        status_url, metrics_url, tag = get_goob_target(server)
        raw_metrics = ""
        try:
            async with bot.session.get(metrics_url, timeout=6, ssl=False) as resp:
                if resp.status == 200:
                    raw_metrics = await resp.text()
        except Exception:
            pass

        ghosts = 0
        m_g = re.search(r"game_director_active_ghosts\s+([0-9.]+)", raw_metrics)
        if m_g:
            ghosts = int(float(m_g.group(1)))

        antags = {}
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_antags{"):
                m = re.match(r'game_director_antags\{rule="([^"]+)"\}\s+([0-9.]+)', line)
                if m:
                    antags[m.group(1)] = int(float(m.group(2)))

        events = {}
        for line in raw_metrics.splitlines():
            if line.startswith("game_director_events_run_total{"):
                m = re.match(r'game_director_events_run_total\{event_name="([^"]+)",beat_name="[^"]*"\}\s+([0-9.]+)', line)
                if m:
                    ev = m.group(1)
                    cnt = int(float(m.group(2)))
                    events[ev] = events.get(ev, 0) + cnt

        ghost_roles_keys = ("spawn", "cyborg", "droppod", "migration", "fugitive", "clone", "ninja", "dragon", "abductor", "revenant", "rat", "pirate", "horde", "critter")
        def is_ghost_role(r: str) -> bool:
            low = r.lower()
            return any(k in low for k in ghost_roles_keys)

        g_roles = {k: v for k, v in antags.items() if is_ghost_role(k) and v > 0}
        main_antags = {k: v for k, v in antags.items() if not is_ghost_role(k) and v > 0}

        msg_lines = [
            f"Обстановка по гост-ролям и призракам на **{tag}**:\n",
            f"- Призраков в лобби (ожидают спавна / роли): **{ghosts}**\n",
            "**Активные заспавненные гост-роли на станции:**"
        ]

        if g_roles:
            for k, v in sorted(g_roles.items(), key=lambda x: x[1], reverse=True):
                msg_lines.append(f"  • **{get_antag_display(k)}**: **{v}**")
        else:
            msg_lines.append("  • `(нет активных гост-ролей на станции)`")

        msg_lines.append("\n**Ивенты спавна ролей / существ (Game Director):**")
        spawn_events = {k: v for k, v in events.items() if is_ghost_role(k)}
        if spawn_events:
            for k, v in sorted(spawn_events.items(), key=lambda x: x[1], reverse=True):
                msg_lines.append(f"  • **{get_antag_display(k)}**: **{v}** раз(а)")
        else:
            msg_lines.append("  • `(ивентов спавна пока не запускалось)`")

        if main_antags:
            msg_lines.append("\n**Другие активные антагонисты:**")
            for k, v in sorted(main_antags.items(), key=lambda x: x[1], reverse=True):
                msg_lines.append(f"  • **{get_antag_display(k)}**: **{v}**")

        await interaction.followup.send("\n".join(msg_lines))
        return

    prom_url = bot.cfg.get("prometheus_url", "http://185.97.255.6:9090")
    r_ghost = await prom_query(prom_url, bot.session, f"ss14_storyteller_available_ghost_roles{{job='{server}'}}")
    ghost_count = r_ghost[0]["value"][1] if r_ghost else "0"

    t = bot.trackers.get(server)
    recent = ""
    if t and t.recent_ghost_antags:
        recent = "\n- Последние взятые боевые роли: " + ", ".join(t.recent_ghost_antags)

    msg = (
        f"Обстановка по гост-ролям на **{server.upper()}**:\n"
        f"- Доступно в меню F7 (свободных мест): **{ghost_count}**{recent}"
    )
    await interaction.followup.send(msg)


def main():
    cfg = load_config()
    token = os.environ.get("DISCORD_TOKEN") or cfg.get("discord_token", "")
    if not token or token.startswith("ВСТАВЬ"):
        print("[-] ОШИБКА: Задай токен бота в bot_config.json!", flush=True)
        sys.exit(1)
    bot.run(token)


if __name__ == "__main__":
    main()
